package io.bluedb.api;

import io.bluedb.api.entities.BlueEntity;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;

public interface BlueDb {

	public void insert(BlueEntity entity) throws BlueDbException;

	public <T extends BlueEntity> T get(Class<T> type, BlueKey key) throws BlueDbException;

	public <T extends BlueEntity> void update(Class<T> type, BlueKey key, Updater<T> updater) throws BlueDbException;

	public void delete(Class<? extends BlueEntity> type, BlueKey key) throws BlueDbException;

	public <T extends BlueEntity> BlueQuery<T> query(Class<T> clazz);



	@SuppressWarnings("unchecked")
	public default <T extends BlueEntity> void update(T entity, Updater<T> updater) throws BlueDbException {
		update((Class<T>) entity.getClass(), entity.getKey(), updater);
	}

	public default void delete(BlueEntity entity) throws BlueDbException {
		delete(entity.getClass(), entity.getKey());
	}
}
