package io.bluedb.api;

import java.util.LinkedList;
import java.util.List;
import io.bluedb.api.entities.BlueEntity;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeKey;

public class BlueQueryImpl<T extends BlueEntity> implements BlueQuery<T> {

	private BlueDbInternal db;
	private Class<T> clazz;
	private List<Condition<T>> conditions = new LinkedList<>();

	public BlueQueryImpl(BlueDbInternal db, Class<T> clazz) {
		this.db = db;
		this.clazz = clazz;
	}

	@Override
	public BlueQuery<T> where(Condition<T> c) {
		if (c != null) {
			conditions.add(c);
		}
		return this;
	}

	@Override
	public BlueQuery<T> afterTime(long time) {
		conditions.add(entity -> isEntityAfterTime(entity, time));
		return this;
	}

	private boolean isEntityAfterTime(T entity, long time) {
		BlueKey key = entity.getKey();
		if (key instanceof TimeKey) {
			return ((TimeKey) key).getTime() > time;
		} else {
			return true;
		}
	}

	@Override
	public BlueQuery<T> afterOrAtTime(long time) {
		conditions.add(entity -> isEntityAfterOrAtTime(entity, time));
		return this;
	}

	private boolean isEntityAfterOrAtTime(T entity, long time) {
		BlueKey key = entity.getKey();
		if (key instanceof TimeKey) {
			return ((TimeKey) key).getTime() >= time;
		} else {
			return true;
		}
	}

	@Override
	public BlueQuery<T> beforeTime(long time) {
		conditions.add(entity -> isEntityBeforeTime(entity, time));
		return this;
	}

	private boolean isEntityBeforeTime(T entity, long time) {
		BlueKey key = entity.getKey();
		if (key instanceof TimeKey) {
			return ((TimeKey) key).getTime() < time;
		} else {
			return true;
		}
	}

	@Override
	public BlueQuery<T> beforeOrAtTime(long time) {
		conditions.add(entity -> isEntityBeforeOrAtTime(entity, time));
		return this;
	}

	private boolean isEntityBeforeOrAtTime(T entity, long time) {
		BlueKey key = entity.getKey();
		if (key instanceof TimeKey) {
			return ((TimeKey) key).getTime() <= time;
		} else {
			return true;
		}
	}

	@Override
	public List<T> getAll() throws BlueDbException {
		return db.getAll(clazz, conditions);
	}

	@Override
	public void deleteAll() throws BlueDbException {
		db.deleteAll(clazz, conditions);
	}

	@Override
	public void updateAll(Updater<T> updater) throws BlueDbException {
		db.updateAll(clazz, conditions, updater);
	}
}
