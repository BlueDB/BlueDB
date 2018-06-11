package io.bluedb.api;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeKey;

public class BlueQueryImpl<T extends Serializable> implements BlueQuery<T> {

	private BlueDbInternal db;
	private Class<T> clazz;
	private List<Condition<BlueKey>> keyConditions = new LinkedList<>();
	private List<Condition<T>> objectConditions = new LinkedList<>();

	public BlueQueryImpl(BlueDbInternal db, Class<T> clazz) {
		this.db = db;
		this.clazz = clazz;
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
		return db.getAll(clazz, keyConditions, objectConditions);
	}

	@Override
	public void deleteAll() throws BlueDbException {
		db.deleteAll(clazz, keyConditions, objectConditions);
	}

	@Override
	public void updateAll(Updater<T> updater) throws BlueDbException {
		db.updateAll(clazz, keyConditions, objectConditions, updater);
	}
}
