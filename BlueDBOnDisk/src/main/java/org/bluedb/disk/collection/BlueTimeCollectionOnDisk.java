package org.bluedb.disk.collection;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.bluedb.api.BlueTimeCollection;
import org.bluedb.api.BlueTimeQuery;
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
import org.bluedb.disk.query.BlueTimeQueryOnDisk;
import org.bluedb.disk.segment.SegmentSizeSetting;

public class BlueTimeCollectionOnDisk<T extends Serializable> extends BlueCollectionOnDisk<T> implements BlueTimeCollection<T> {

	public BlueTimeCollectionOnDisk(BlueDbOnDisk db, String name, Class<? extends BlueKey> requestedKeyType, Class<T> valueType, List<Class<? extends Serializable>> additionalRegisteredClasses, SegmentSizeSetting segmentSize) throws BlueDbException {
		super(db, name, requestedKeyType, valueType, additionalRegisteredClasses, segmentSize);
	}

	@Override
	public <I extends ValueKey> BlueIndex<I, T> createIndex(String name, Class<I> keyType, KeyExtractor<I, T> keyExtractor) throws BlueDbException {
		return indexManager.getOrCreate(name, keyType, keyExtractor);
	}

	@Override
	public BlueTimeQuery<T> query() {
		return new BlueTimeQueryOnDisk<T>(this);
	}

	//TODO: Remember that this is duplicated so you might want to pull of some strategy pattern shiz to share code here
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

}
