package io.bluedb.disk;

import java.io.Serializable;
import java.util.List;
import io.bluedb.api.BlueDbInternal;
import io.bluedb.api.BlueQuery;
import io.bluedb.api.Condition;
import io.bluedb.api.Updater;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;

public class BlueDBOnDiskImpl implements BlueDbInternal {

	@Override
	public void insert(Serializable object, BlueKey key) throws BlueDbException {
		// TODO Auto-generated method stub

	}

	@Override
	public <T extends Serializable> T get(Class<T> type, BlueKey key) throws BlueDbException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Serializable> void update(Class<T> type, BlueKey key, Updater<T> updater) throws BlueDbException {
		// TODO Auto-generated method stub

	}

	@Override
	public void delete(Class<? extends Serializable> type, BlueKey key) throws BlueDbException {
		// TODO Auto-generated method stub

	}

	@Override
	public <T extends Serializable> BlueQuery<T> query(Class<T> clazz) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Serializable> List<T> getAll(Class<T> clazz, List<Condition<BlueKey>> keyConditions, List<Condition<T>> objectConditions) throws BlueDbException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Serializable> void deleteAll(Class<T> clazz, List<Condition<BlueKey>> keyConditions, List<Condition<T>> objectConditions) throws BlueDbException {
		// TODO Auto-generated method stub

	}

	@Override
	public <T extends Serializable> void updateAll(Class<T> clazz, List<Condition<BlueKey>> keyConditions, List<Condition<T>> objectConditions, Updater<T> updater) throws BlueDbException {
		// TODO Auto-generated method stub

	}

}
