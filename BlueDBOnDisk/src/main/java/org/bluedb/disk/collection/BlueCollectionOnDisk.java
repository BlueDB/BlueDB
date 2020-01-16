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
import org.bluedb.disk.BlueDbOnDisk;
import org.bluedb.disk.collection.task.BatchChangeTask;
import org.bluedb.disk.collection.task.BatchDeleteTask;
import org.bluedb.disk.collection.task.DeleteTask;
import org.bluedb.disk.collection.task.InsertTask;
import org.bluedb.disk.collection.task.ReplaceTask;
import org.bluedb.disk.collection.task.UpdateTask;
import org.bluedb.disk.executors.BlueExecutor;
import org.bluedb.disk.query.BlueQueryOnDisk;
import org.bluedb.disk.recovery.RecoveryManager;
import org.bluedb.disk.segment.SegmentSizeSetting;
import org.bluedb.disk.segment.rollup.RollupScheduler;
import org.bluedb.disk.segment.rollup.RollupTarget;
import org.bluedb.disk.segment.rollup.Rollupable;

public class BlueCollectionOnDisk<T extends Serializable> extends ReadOnlyBlueCollectionOnDisk<T> implements BlueCollection<T>, Rollupable {

	private final BlueExecutor sharedExecutor;
	private final String collectionKey;
	private final RollupScheduler rollupScheduler;
	private final RecoveryManager<T> recoveryManager;

	public BlueCollectionOnDisk(BlueDbOnDisk db, String name, Class<? extends BlueKey> requestedKeyType, Class<T> valueType, List<Class<? extends Serializable>> additionalRegisteredClasses) throws BlueDbException {
		super(db, name, requestedKeyType, valueType, additionalRegisteredClasses);
		sharedExecutor = db.getSharedExecutor();
		collectionKey = getPath().toString();
		rollupScheduler = new RollupScheduler(this);
		rollupScheduler.start();
		recoveryManager = new RecoveryManager<T>(this, getFileManager(), getSerializer());
		recoveryManager.recover();  // everything else has to be in place before running this
	}

	public BlueCollectionOnDisk(BlueDbOnDisk db, String name, Class<? extends BlueKey> requestedKeyType, Class<T> valueType, List<Class<? extends Serializable>> additionalRegisteredClasses, SegmentSizeSetting segmentSize) throws BlueDbException {
		super(db, name, requestedKeyType, valueType, additionalRegisteredClasses, segmentSize);
		sharedExecutor = db.getSharedExecutor();
		collectionKey = getPath().toString();
		rollupScheduler = new RollupScheduler(this);
		rollupScheduler.start();
		recoveryManager = new RecoveryManager<T>(this, getFileManager(), getSerializer());
		recoveryManager.recover();  // everything else has to be in place before running this
	}
	
	@Override
	public <I extends ValueKey> BlueIndex<I, T> createIndex(String name, Class<I> keyType, KeyExtractor<I, T> keyExtractor) throws BlueDbException {
		return indexManager.getOrCreate(name, keyType, keyExtractor);
	}

	@Override
	public BlueQuery<T> query() {
		return new BlueQueryOnDisk<T>(this);
	}

	@Override
	public void insert(BlueKey key, T value) throws BlueDbException {
		ensureCorrectKeyType(key);
		Runnable insertTask = new InsertTask<T>(this, key, value);
		executeTask(insertTask);
	}

	//TODO: Remember that this is duplicated so you might want to pull of some strategy pattern shiz to share code here
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
}
