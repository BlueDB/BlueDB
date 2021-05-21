package org.bluedb.disk.collection;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.BlueQuery;
import org.bluedb.api.Mapper;
import org.bluedb.api.Updater;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.ReadWriteDbOnDisk;
import org.bluedb.disk.collection.index.ReadWriteIndexOnDisk;
import org.bluedb.disk.collection.index.ReadWriteIndexManager;
import org.bluedb.disk.collection.metadata.ReadWriteCollectionMetaData;
import org.bluedb.disk.collection.task.BatchChangeTask;
import org.bluedb.disk.collection.task.BatchDeleteTask;
import org.bluedb.disk.collection.task.DeleteTask;
import org.bluedb.disk.collection.task.InsertTask;
import org.bluedb.disk.collection.task.ReplaceTask;
import org.bluedb.disk.collection.task.UpdateTask;
import org.bluedb.disk.executors.BlueExecutor;
import org.bluedb.disk.file.ReadWriteFileManager;
import org.bluedb.disk.query.QueryOnDisk;
import org.bluedb.disk.recovery.RecoveryManager;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadWriteSegment;
import org.bluedb.disk.segment.ReadWriteSegmentManager;
import org.bluedb.disk.segment.SegmentSizeSetting;
import org.bluedb.disk.segment.rollup.RollupScheduler;
import org.bluedb.disk.segment.rollup.RollupTarget;
import org.bluedb.disk.segment.rollup.Rollupable;

public class ReadWriteCollectionOnDisk<T extends Serializable> extends ReadableCollectionOnDisk<T> implements BlueCollection<T>, Rollupable {

	private final BlueExecutor sharedExecutor;
	private final String collectionKey;
	private final RollupScheduler rollupScheduler;
	private final RecoveryManager<T> recoveryManager;
	private final ReadWriteFileManager fileManager;
	private final ReadWriteSegmentManager<T> segmentManager;
	protected final ReadWriteIndexManager<T> indexManager;

	public ReadWriteCollectionOnDisk(ReadWriteDbOnDisk db, String name, Class<? extends BlueKey> requestedKeyType, Class<T> valueType, List<Class<? extends Serializable>> additionalRegisteredClasses) throws BlueDbException {
		this(db, name, requestedKeyType, valueType, additionalRegisteredClasses, null);
	}

	public ReadWriteCollectionOnDisk(ReadWriteDbOnDisk db, String name, Class<? extends BlueKey> requestedKeyType, Class<T> valueType, List<Class<? extends Serializable>> additionalRegisteredClasses, SegmentSizeSetting segmentSize) throws BlueDbException {
		super(db, name, requestedKeyType, valueType, additionalRegisteredClasses, segmentSize);
		sharedExecutor = db.getSharedExecutor();
		collectionKey = getPath().toString();
		rollupScheduler = new RollupScheduler(this);
		rollupScheduler.start();
		fileManager = new ReadWriteFileManager(serializer);
		recoveryManager = new RecoveryManager<T>(this, getFileManager(), getSerializer());
		Rollupable rollupable = this;
		indexManager = new ReadWriteIndexManager<T>(this, collectionPath);
		if (segmentSizeSettings == null) {
			segmentManager = null;
			return;
		}
		segmentManager = new ReadWriteSegmentManager<T>(collectionPath, fileManager, rollupable, segmentSizeSettings.getConfig());
		recoveryManager.recover();  // everything else has to be in place before running this
	}
	
	@Override
	public ReadWriteFileManager getFileManager() {
		return fileManager;
	}
	
	public ReadWriteIndexManager<T> getIndexManager() {
		return indexManager;
	}

	@Override
	public ReadWriteSegmentManager<T> getSegmentManager() {
		return segmentManager;
	}

	@Override
	public <I extends ValueKey> BlueIndex<I, T> createIndex(String name, Class<I> keyType, KeyExtractor<I, T> keyExtractor) throws BlueDbException {
		return indexManager.getOrCreate(name, keyType, keyExtractor);
	}

	@Override
	public BlueQuery<T> query() {
		return new QueryOnDisk<T>(this);
	}

	@Override
	public void insert(BlueKey key, T value) throws BlueDbException {
		ensureCorrectKeyType(key);
		Runnable insertTask = new InsertTask<T>(this, key, value);
		executeTask(insertTask);
	}

	@Override
	public void batchUpsert(Map<BlueKey, T> values) throws BlueDbException {
		ensureCorrectKeyTypes(values.keySet());
		Runnable insertTask = new BatchChangeTask<T>(this, values);
		executeTask(insertTask);
	}

	@Override
	public void batchDelete(Collection<BlueKey> keys) throws BlueDbException {
		ensureCorrectKeyTypes(keys);
		Runnable deleteTask = new BatchDeleteTask<T>(this, keys);
		executeTask(deleteTask);
	}

	@Override
	public void replace(BlueKey key, Mapper<T> mapper) throws BlueDbException {
		ensureCorrectKeyType(key);
		Runnable updateTask = new ReplaceTask<T>(this, key, mapper);
		executeTask(updateTask);
	}

	@Override
	public void update(BlueKey key, Updater<T> updater) throws BlueDbException {
		ensureCorrectKeyType(key);
		Runnable updateTask = new UpdateTask<T>(this, key, updater);
		executeTask(updateTask);
	}

	@Override
	public void delete(BlueKey key) throws BlueDbException {
		ensureCorrectKeyType(key);
		Runnable deleteTask = new DeleteTask<T>(this, key);
		executeTask(deleteTask);
	}

	public int getQueuedTaskCount() {
		return sharedExecutor.getQueryQueueSize(collectionKey);
	}

	public BlueExecutor getSharedExecutor() {
		return sharedExecutor;
	}

	public void submitTask(Runnable task) {
		sharedExecutor.submitQueryTask(collectionKey, task);
	}

	public void executeTask(Runnable task) throws BlueDbException{
		Future<?> future = sharedExecutor.submitQueryTask(collectionKey, task);
		try {
			future.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			throw new BlueDbException("BlueDB task failed " + task.toString(), e);
		}
	}

	public RecoveryManager<T> getRecoveryManager() {
		return recoveryManager;
	}

	@Override
	public void reportReads(List<RollupTarget> rollupTargets) {
		rollupScheduler.reportReads(rollupTargets);
	}

	@Override
	public void reportWrites(List<RollupTarget> rollupTargets) {
		rollupScheduler.reportWrites(rollupTargets);
	}

	public RollupScheduler getRollupScheduler() {
		return rollupScheduler;
	}

	public ReadWriteCollectionMetaData getMetaData() {
		return metadata;
	}

	private ReadWriteCollectionMetaData metadata;

	@Override
	protected ReadWriteCollectionMetaData getOrCreateMetadata() {
		if (metadata == null) {
			metadata = new ReadWriteCollectionMetaData(getPath());
		}
		return metadata;
	}

	@Override
	protected Class<? extends Serializable>[] getClassesToRegister(List<Class<? extends Serializable>> additionalRegisteredClasses) throws BlueDbException {
		return metadata.getAndAddToSerializedClassList(getType(), additionalRegisteredClasses);

	}

	public void rollup(Range timeRange) throws BlueDbException {
		ReadWriteSegment<T> segment = segmentManager.getSegment(timeRange.getStart());
		segment.rollup(timeRange);
	}


	public void rollupIndex(String indexName, Range range) throws BlueDbException {
		ReadWriteIndexOnDisk<?, T> index = indexManager.getUntypedIndex(indexName);
		if (index != null) {
			index.rollup(range);
		}
	}

	//TODO: getIndex needs to work even if they haven't called initialize or build. Return empty index object if it doesn't exist
	@Override
	public <I extends ValueKey> ReadWriteIndexOnDisk<I, T> getIndex(String indexName, Class<I> keyType) throws BlueDbException {
		return indexManager.getIndex(indexName, keyType);
	}
}
