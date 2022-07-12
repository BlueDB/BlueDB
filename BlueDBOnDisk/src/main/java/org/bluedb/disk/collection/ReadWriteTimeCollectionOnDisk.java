package org.bluedb.disk.collection;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.bluedb.api.BlueCollectionVersion;
import org.bluedb.api.BlueTimeCollection;
import org.bluedb.api.BlueTimeQuery;
import org.bluedb.api.Mapper;
import org.bluedb.api.TimeEntityMapper;
import org.bluedb.api.TimeEntityUpdater;
import org.bluedb.api.Updater;
import org.bluedb.api.datastructures.BlueKeyValuePair;
import org.bluedb.api.datastructures.TimeKeyValuePair;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.IteratorWrapper;
import org.bluedb.disk.ReadWriteDbOnDisk;
import org.bluedb.disk.IteratorWrapper.IteratorWrapperMapper;
import org.bluedb.disk.collection.task.BatchUpsertEntitiesTask;
import org.bluedb.disk.collection.task.SingleRecordChangeTask;
import org.bluedb.disk.collection.task.SingleRecordChangeTask.SingleRecordChangeMode;
import org.bluedb.disk.query.TimeQueryOnDisk;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.recovery.KeyValueToChangeMapper;
import org.bluedb.disk.segment.SegmentSizeSetting;

public class ReadWriteTimeCollectionOnDisk<T extends Serializable> extends ReadWriteCollectionOnDisk<T> implements BlueTimeCollection<T> {

	public ReadWriteTimeCollectionOnDisk(ReadWriteDbOnDisk db, String name, BlueCollectionVersion requestedVersion, Class<? extends BlueKey> requestedKeyType, Class<T> valueType, List<Class<? extends Serializable>> additionalRegisteredClasses, SegmentSizeSetting segmentSize) throws BlueDbException {
		super(db, name, requestedVersion, requestedKeyType, valueType, additionalRegisteredClasses, segmentSize);
	}

	@Override
	public BlueTimeQuery<T> query() {
		return new TimeQueryOnDisk<T>(this);
	}
	
	@Override
	public void batchUpsertKeysAndValues(Map<BlueKey, T> values) throws BlueDbException {
		IteratorWrapperMapper<Entry<BlueKey, T>, BlueKeyValuePair<T>> mapper = entry -> new BlueKeyValuePair<T>(entry.getKey(), entry.getValue());
		Iterator<BlueKeyValuePair<T>> keyValuePairIterator = new IteratorWrapper<>(values.entrySet().iterator(), mapper);
		
		String description = "BatchUpsertKeysAndValues map of size " + values.size();
		Runnable task = new BatchUpsertEntitiesTask<T>(description, this, keyValuePairIterator);
		executeTask(task);
	}
	
	@Override
	public void batchUpsertKeysAndValues(Iterator<BlueKeyValuePair<T>> keyValuePairIterator) throws BlueDbException {
		String description = "BatchUpsertKeysAndValues using an iterator of key value pairs";
		Runnable task = new BatchUpsertEntitiesTask<T>(description, this, keyValuePairIterator);
		executeTask(task);
	}

	@Override
	public void replaceKeyAndValue(TimeKey key, Mapper<T> mapper) throws BlueDbException {
		ensureCorrectKeyType(key);
		
		KeyValueToChangeMapper<T> changeMapper = getReplaceKeyValueToChangeMapper(key, mapper);
		
		String description = "ReplaceKeyAndValue [key]" + key;
		Runnable task = new SingleRecordChangeTask<>(description, this, key, changeMapper, SingleRecordChangeMode.REQUIRE_ALREADY_EXISTS);
		executeTask(task);
	}
	
	private KeyValueToChangeMapper<T> getReplaceKeyValueToChangeMapper(TimeKey newTimeKey, Mapper<T> mapper) {
		TimeEntityMapper<T> timeEntityMapper = originalValue -> {
			T newValue = mapper.update(originalValue);
			return new TimeKeyValuePair<T>(newTimeKey, newValue);
		};
		return (originalKey, originalvalue) -> {
			return IndividualChange.createReplaceKeyAndValueChange(originalKey, originalvalue, timeEntityMapper, serializer);
		};
	}

	@Override
	public void updateKeyAndValue(TimeKey key, Updater<T> updater) throws BlueDbException {
		ensureCorrectKeyType(key);
		
		KeyValueToChangeMapper<T> changeMapper = getUpdateKeyValueToChangeMapper(key, updater);
		
		String description = "UpdateKeyAndValue [key]" + key;
		Runnable task = new SingleRecordChangeTask<>(description, this, key, changeMapper, SingleRecordChangeMode.REQUIRE_ALREADY_EXISTS);
		executeTask(task);
	}
	
	private KeyValueToChangeMapper<T> getUpdateKeyValueToChangeMapper(TimeKey newTimeKey, Updater<T> updater) {
		TimeEntityUpdater<T> timeEntityUpdater = originalValue -> {
			updater.update(originalValue);
			return newTimeKey;
		};
		return (originalKey, originalvalue) -> {
			return IndividualChange.createUpdateKeyAndValueChange(originalKey, originalvalue, timeEntityUpdater, serializer);
		};
	}

}
