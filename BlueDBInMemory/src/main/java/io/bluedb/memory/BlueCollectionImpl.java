package io.bluedb.memory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.nustaq.serialization.FSTConfiguration;
import io.bluedb.api.BlueCollection;
import io.bluedb.api.BlueQuery;
import io.bluedb.api.Condition;
import io.bluedb.api.Updater;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.api.keys.TimeKey;

class BlueCollectionImpl<T extends Serializable> implements BlueCollection<T> {

	private Class<T> type;
	private Map<BlueKey, byte[]> data = new ConcurrentHashMap<>();
	FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();

	public BlueCollectionImpl(Class<T> type) {
		this.type = type;
	}

	@Override
	public void insert(BlueKey key, T object) throws BlueDbException {
		// TODO lock on update, insert or delete
		byte[] bytes = serialize(object);
		data.put(key, bytes);
	}

	@Override
	public T get(BlueKey key) throws BlueDbException {
		byte[] bytes = data.get(key);
		if (bytes == null)
			return null;
		return deserialize(bytes);
	}

	@Override
	public void update(BlueKey key, Updater<T> updater) throws BlueDbException {
		// TODO lock on update, insert or delete
		byte[] bytes = data.get(key);
		if (bytes != null) {
			T object = deserialize(bytes);
			updater.update(object);
			bytes = serialize(object);
			data.put(key, bytes);
		}
	}

	@Override
	public void delete(BlueKey key) throws BlueDbException {
		data.remove(key);
	}

	@Override
	public BlueQuery<T> query() {
		return new BlueQueryImpl<T>(this);
	}

	public List<T> getList(long minTime, long maxTime, List<Condition<T>> objectConditions) {
		List<T> results = new ArrayList<>();
		List<BlueKey> matches = findMatches(minTime, maxTime, objectConditions);
		for (BlueKey key: matches) {
			byte[] bytes = data.get(key);
			if (bytes != null) { // in case there's been a delete
				T object = deserialize(bytes);
				results.add(object);
			}
		}
		return results;
	}

	public void deleteAll(long minTime, long maxTime, List<Condition<T>> objectConditions) throws BlueDbException {
		List<BlueKey> matches = findMatches(minTime, maxTime, objectConditions);
		for (BlueKey key: matches) {
			// TODO lock on update, insert or delete
			data.remove(key);
		}
	}

	public void updateAll(long minTime, long maxTime, List<Condition<T>> objectConditions, Updater<T> updater) throws BlueDbException {
		List<BlueKey> matches = findMatches(minTime, maxTime, objectConditions);
		for (BlueKey key: matches) {
			// TODO lock on update, insert or delete
			byte[] bytes = data.get(key);
			T obj = deserialize(bytes);
			updater.update(obj);
			bytes = serialize(obj);
			data.put(key, bytes);
		}
	}

	private List<BlueKey> findMatches(long minTime, long maxTime, List<Condition<T>> objectConditions) {
		List<BlueKey> results = new ArrayList<>();
		for (BlueKey key: data.keySet()) {
			if (inTimeRange(minTime, maxTime, key) && meetsConditions(objectConditions, data.get(key))) {
				results.add(key);
			}
		}
		return results;
	}
	
	private boolean inTimeRange(long minTime, long maxTime, BlueKey key) {
		if (key instanceof TimeFrameKey) {
			TimeFrameKey timeFrameKey = (TimeFrameKey) key;
			return timeFrameKey.getEndTime() >= minTime && timeFrameKey.getStartTime() <= maxTime;
		} else if (key instanceof TimeKey) {
			long time = ((TimeKey) key).getTime();
			return time >= minTime && time <= maxTime;
		} else {
			return true;
		}
	}

	private <X extends Serializable> boolean meetsConditions(List<Condition<X>> conditions, byte[] bytes) {
		if (bytes == null) { // in case it's been deleted.
			return false;
		}
		@SuppressWarnings("unchecked")
		X object = (X) deserialize(bytes);
		return meetsConditions(conditions, object);
	}

	private <X extends Serializable> boolean meetsConditions(List<Condition<X>> conditions, X object) {
		for (Condition<X> condition: conditions) {
			if (!condition.resolve(object)) {
				return false;
			}
		}
		return true;
	}

	private byte[] serialize(T object) {
		return conf.asByteArray(object);
	}

	@SuppressWarnings("unchecked")
	private T deserialize(byte[] bytes) {
		return (T) conf.asObject(bytes);
	}
}
