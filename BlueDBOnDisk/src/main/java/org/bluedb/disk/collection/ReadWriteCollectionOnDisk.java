package org.bluedb.disk.collection;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.BlueQuery;
import org.bluedb.api.Mapper;
import org.bluedb.api.Updater;
import org.bluedb.api.datastructures.BlueKeyValuePair;
import org.bluedb.api.datastructures.BlueSimpleInMemorySet;
import org.bluedb.api.datastructures.BlueSimpleSet;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.BlueIndexInfo;
import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.IteratorWrapper;
import org.bluedb.disk.ReadWriteDbOnDisk;
import org.bluedb.disk.IteratorWrapper.IteratorWrapperMapper;
import org.bluedb.disk.collection.index.ReadWriteIndexManager;
import org.bluedb.disk.collection.index.ReadWriteIndexOnDisk;
import org.bluedb.disk.collection.metadata.ReadWriteCollectionMetaData;
import org.bluedb.disk.collection.task.BatchIteratorChangeTask;
import org.bluedb.disk.collection.task.BatchQueryChangeTask;
import org.bluedb.disk.collection.task.SingleRecordChangeTask;
import org.bluedb.disk.collection.task.SingleRecordChangeTask.SingleRecordChangeMode;
import org.bluedb.disk.executors.BlueExecutor;
import org.bluedb.disk.file.ReadWriteFileManager;
import org.bluedb.disk.query.QueryOnDisk;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.recovery.KeyValueToChangeMapper;
import org.bluedb.disk.recovery.RecoveryManager;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadWriteSegment;
import org.bluedb.disk.segment.ReadWriteSegmentManager;
import org.bluedb.disk.segment.SegmentSizeSetting;
import org.bluedb.disk.segment.rollup.RollupScheduler;
import org.bluedb.disk.segment.rollup.RollupTarget;
import org.bluedb.disk.segment.rollup.Rollupable;
import org.bluedb.disk.serialization.validation.ObjectValidation;

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
		fileManager = new ReadWriteFileManager(serializer, db.getEncryptionService());
		recoveryManager = new RecoveryManager<T>(this, getFileManager(), getSerializer());
		Rollupable rollupable = this;
		indexManager = new ReadWriteIndexManager<T>(this, collectionPath);
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
	public void createIndices(Collection<BlueIndexInfo<? extends ValueKey, T>> indexInfo) throws BlueDbException {
		indexManager.createIndices(indexInfo);
	}

	@Override
	public BlueQuery<T> query() {
		return new QueryOnDisk<T>(this);
	}

	@Override
	public void insert(BlueKey key, T value) throws BlueDbException {
		ensureCorrectKeyType(key);
		ObjectValidation.validateFieldValueTypesForObject(value);
		
		KeyValueToChangeMapper<T> changeMapper = (originalKey, originalvalue) -> {
			return IndividualChange.createInsertChange(key, value);
		};
		
		String description = "Insert [key]" + key + " [value]" + value;
		Runnable task = new SingleRecordChangeTask<>(description, this, key, changeMapper, SingleRecordChangeMode.REQUIRE_DOES_NOT_ALREADY_EXIST);
		executeTask(task);
	}

	@Override
	public void batchUpsert(Map<BlueKey, T> values) throws BlueDbException {
		IteratorWrapperMapper<Entry<BlueKey, T>, IndividualChange<T>> mapper = IndividualChange::createInsertChange;
		Iterator<IndividualChange<T>> changeIterator = new IteratorWrapper<>(values.entrySet().iterator(), mapper)
				.addValidator(keyValuePair -> ensureCorrectKeyType(keyValuePair.getKey()));
		
		String description = "BatchUpsert map of size " + values.size();
		Runnable task = new BatchIteratorChangeTask<>(description, this, changeIterator);
		executeTask(task);
	}
	
	@Override
	public void batchUpsert(Iterator<BlueKeyValuePair<T>> keyValuePairIterator) throws BlueDbException {
		IteratorWrapperMapper<BlueKeyValuePair<T>, IndividualChange<T>> mapper = IndividualChange::createInsertChange;
		Iterator<IndividualChange<T>> changeIterator = new IteratorWrapper<>(keyValuePairIterator, mapper)
				.addValidator(keyValuePair -> ensureCorrectKeyType(keyValuePair.getKey()));
		
		String description = "BatchUpsert using an iterator of key value pairs";
		Runnable task = new BatchIteratorChangeTask<>(description, this, changeIterator);
		executeTask(task);
	} 

	@Override
	public void batchDelete(Collection<BlueKey> keys) throws BlueDbException {
		Set<BlueKey> keySet = new HashSet<>(keys);
		batchDelete(new BlueSimpleInMemorySet<>(keySet));
	}
	
	@Override
	public void batchDelete(BlueSimpleSet<BlueKey> keys) throws BlueDbException {
		String description = "BatchDelete keys";
		QueryOnDisk<T> query = new QueryOnDisk<>(this);
		query.whereKeyIsIn(keys);
		
		Runnable task = new BatchQueryChangeTask<T>(description, this, query, IndividualChange::createDeleteChange);
		executeTask(task);
	}

	@Override
	public void replace(BlueKey key, Mapper<T> mapper) throws BlueDbException {
		ensureCorrectKeyType(key);
		
		KeyValueToChangeMapper<T> changeMapper = (originalKey, originalvalue) -> {
			return IndividualChange.createReplaceChange(originalKey, originalvalue, mapper, serializer);
		};
		
		String description = "Replace [key]" + key;
		Runnable task = new SingleRecordChangeTask<>(description, this, key, changeMapper, SingleRecordChangeMode.REQUIRE_ALREADY_EXISTS);
		executeTask(task);
	}

	@Override
	public void update(BlueKey key, Updater<T> updater) throws BlueDbException {
		ensureCorrectKeyType(key);
		
		KeyValueToChangeMapper<T> changeMapper = (originalKey, originalvalue) -> {
			return IndividualChange.createUpdateChange(originalKey, originalvalue, updater, serializer);
		};
		
		String description = "Update [key]" + key;
		Runnable task = new SingleRecordChangeTask<>(description, this, key, changeMapper, SingleRecordChangeMode.REQUIRE_ALREADY_EXISTS);
		executeTask(task);
	}

	@Override
	public void delete(BlueKey key) throws BlueDbException {
		ensureCorrectKeyType(key);
		
		String description = "Delete [key]" + key;
		Runnable task = new SingleRecordChangeTask<T>(description, this, key, IndividualChange::createDeleteChange, SingleRecordChangeMode.NO_REQUIREMENTS);
		executeTask(task);
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
			metadata = new ReadWriteCollectionMetaData(getPath(), this.encryptionService);
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
