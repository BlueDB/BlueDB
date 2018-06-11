package io.bluedb.memory;

import java.io.Serializable;
import java.util.List;
import io.bluedb.api.BlueDbInternalCollection;
import io.bluedb.api.BlueQuery;
import io.bluedb.api.Condition;
import io.bluedb.api.Updater;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;

public class BlueDbInMemoryCollection<T extends Serializable> implements BlueDbInternalCollection<T> {

	private Class<T> type;

	public BlueDbInMemoryCollection(Class<T> type) {
		this.type = type;
	}

	@Override
	public void insert(T object, BlueKey key) throws BlueDbException {
		// TODO Auto-generated method stub

	}

	@Override
	public T get(BlueKey key) throws BlueDbException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void update(BlueKey key, Updater<T> updater) throws BlueDbException {
		// TODO Auto-generated method stub

	}

	@Override
	public void delete(BlueKey key) throws BlueDbException {
		// TODO Auto-generated method stub

	}

	@Override
	public BlueQuery<T> query() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<T> getAll(List<Condition<BlueKey>> keyConditions, List<Condition<T>> objectConditions) throws BlueDbException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void deleteAll(List<Condition<BlueKey>> keyConditions, List<Condition<T>> objectConditions) throws BlueDbException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateAll(List<Condition<BlueKey>> keyConditions, List<Condition<T>> objectConditions, Updater<T> updater) throws BlueDbException {
		// TODO Auto-generated method stub

	}

}
