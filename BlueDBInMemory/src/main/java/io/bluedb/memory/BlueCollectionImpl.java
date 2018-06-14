package io.bluedb.memory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
	private Map<BlueKey, T> data = new HashMap<>();

	public BlueCollectionImpl(Class<T> type) {
		this.type = type;
	}

	@Override
	public void insert(BlueKey key, T object) throws BlueDbException {
		object = clone(object);
		data.put(key, object); // TODO store bytes instead to imitate onDisk
	}

	@Override
	public T get(BlueKey key) throws BlueDbException {
		return clone(data.get(key)); // TODO store bytes instead to imitate onDisk
	}

	@Override
	public void update(BlueKey key, Updater<T> updater) throws BlueDbException {
		T object = data.get(key);
		if (object != null) {
			updater.update(object);
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
			results.add(clone(data.get(key))); // TODO store bytes instead to imitate onDisk
		}
		return results;
	}

	public void deleteAll(long minTime, long maxTime, List<Condition<T>> objectConditions) throws BlueDbException {
		List<BlueKey> matches = findMatches(minTime, maxTime, objectConditions);
		for (BlueKey key: matches) {
			data.remove(key);
		}
	}

	public void updateAll(long minTime, long maxTime, List<Condition<T>> objectConditions, Updater<T> updater) throws BlueDbException {
		List<BlueKey> matches = findMatches(minTime, maxTime, objectConditions);
		for (BlueKey key: matches) {
			updater.update(data.get(key));
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

	private static <X extends Serializable> boolean meetsConditions(List<Condition<X>> conditions, X object) {
		for (Condition<X> condition: conditions) {
			if (!condition.resolve(object)) {
				return false;
			}
		}
		return true;
	}

	 // TODO store bytes instead to imitate onDisk
	private static <X extends Serializable> X clone(X object) {
		FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();
		byte[] serialized = conf.asByteArray(object);
		X deserialized = (X) conf.asObject(serialized);
		return deserialized;
	}
}
