package io.bluedb.disk;

import java.util.List;
import io.bluedb.api.BlueDbInternal;
import io.bluedb.api.BlueQuery;
import io.bluedb.api.Condition;
import io.bluedb.api.Updater;
import io.bluedb.api.entities.BlueEntity;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;

public class BlueDBOnDiskImpl implements BlueDbInternal {

	@Override
	public void insert(BlueEntity entity) throws BlueDbException {
		// TODO Auto-generated method stub

	}

	@Override
	public <T extends BlueEntity> T get(Class<T> type, BlueKey key) throws BlueDbException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends BlueEntity> void update(Class<T> type, BlueKey key, Updater<T> updater) throws BlueDbException {
		// TODO Auto-generated method stub

	}

	@Override
	public void delete(Class<? extends BlueEntity> type, BlueKey key) throws BlueDbException {
		// TODO Auto-generated method stub

	}

	@Override
	public <T extends BlueEntity> BlueQuery<T> query(Class<T> clazz) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends BlueEntity> List<T> getAll(Class<T> clazz, List<Condition<T>> conditions) throws BlueDbException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends BlueEntity> void deleteAll(Class<T> clazz, List<Condition<T>> conditions) throws BlueDbException {
		// TODO Auto-generated method stub

	}

	@Override
	public <T extends BlueEntity> void updateAll(Class<T> clazz, List<Condition<T>> conditions, Updater<T> updater) throws BlueDbException {
		// TODO Auto-generated method stub

	}

}
