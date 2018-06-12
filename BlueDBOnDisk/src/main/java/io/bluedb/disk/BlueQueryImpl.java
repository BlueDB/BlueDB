package io.bluedb.disk;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import io.bluedb.api.BlueQuery;
import io.bluedb.api.Condition;
import io.bluedb.api.Updater;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeKey;

public class BlueQueryImpl<T extends Serializable> implements BlueQuery<T> {

	private BlueCollectionImpl<T> collection;
	private List<Condition<BlueKey>> keyConditions = new LinkedList<>();
	private List<Condition<T>> objectConditions = new LinkedList<>();

	public BlueQueryImpl(BlueCollectionImpl<T> collection) {
		this.collection = collection;
	}

	@Override
	public BlueQuery<T> where(Condition<T> c) {
		if (c != null) {
			objectConditions.add(c);
		}
		return this;
	}

	@Override
	public BlueQuery<T> afterTime(long time) {
		keyConditions.add(key -> isEntityAfterTime(key, time));
		return this;
	}

	private boolean isEntityAfterTime(BlueKey key, long time) {
		if (key instanceof TimeKey) {
			return ((TimeKey) key).getTime() > time;
		} else {
			return true;
		}
	}

	@Override
	public BlueQuery<T> afterOrAtTime(long time) {
		keyConditions.add(key -> isEntityAfterOrAtTime(key, time));
		return this;
	}

	private boolean isEntityAfterOrAtTime(BlueKey key, long time) {
		if (key instanceof TimeKey) {
			return ((TimeKey) key).getTime() >= time;
		} else {
			return true;
		}
	}

	@Override
	public BlueQuery<T> beforeTime(long time) {
		keyConditions.add(key -> isEntityBeforeTime(key, time));
		return this;
	}

	private boolean isEntityBeforeTime(BlueKey key, long time) {
		if (key instanceof TimeKey) {
			return ((TimeKey) key).getTime() < time;
		} else {
			return true;
		}
	}

	@Override
	public BlueQuery<T> beforeOrAtTime(long time) {
		keyConditions.add(key -> isEntityBeforeOrAtTime(key, time));
		return this;
	}

	private boolean isEntityBeforeOrAtTime(BlueKey key, long time) {
		if (key instanceof TimeKey) {
			return ((TimeKey) key).getTime() <= time;
		} else {
			return true;
		}
	}

	@Override
	public List<T> getAll() throws BlueDbException {
		return collection.getAll(keyConditions, objectConditions);
	}

	@Override
	public void deleteAll() throws BlueDbException {
		collection.deleteAll(keyConditions, objectConditions);
	}

	@Override
	public void updateAll(Updater<T> updater) throws BlueDbException {
		collection.updateAll(keyConditions, objectConditions, updater);
	}
}
