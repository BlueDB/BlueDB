package io.bluedb.memory;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import io.bluedb.api.BlueCollection;
import io.bluedb.api.BlueQuery;
import io.bluedb.api.Condition;
import io.bluedb.api.Updater;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;

class BlueCollectionImpl<T extends Serializable> implements BlueCollection<T> {

	private Class<T> type;

	public BlueCollectionImpl(Class<T> type) {
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

	public List<T> getList() {
		// TODO Auto-generated method stub
		return null;
	}

	public Iterator<T> getIterator() {
		// TODO Auto-generated method stub
		return null;
	}

	public void deleteAll(List<Condition<BlueKey>> keyConditions, List<Condition<T>> objectConditions) throws BlueDbException {
		// TODO Auto-generated method stub

	}

	public void updateAll(List<Condition<BlueKey>> keyConditions, List<Condition<T>> objectConditions, Updater<T> updater) throws BlueDbException {
		// TODO Auto-generated method stub

	}

}
