package io.bluedb.api;

import java.io.Serializable;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;

public interface BlueDb {

	public void insert(Serializable object, BlueKey key) throws BlueDbException;

	public <T extends Serializable> T get(Class<T> type, BlueKey key) throws BlueDbException;

	public <T extends Serializable> void update(Class<T> type, BlueKey key, Updater<T> updater) throws BlueDbException;

	public void delete(Class<? extends Serializable> type, BlueKey key) throws BlueDbException;

	public <T extends Serializable> BlueQuery<T> query(Class<T> clazz);
}
