package io.bluedb.api;

import java.io.Serializable;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;

public interface BlueDbCollection<T extends Serializable> {

	public void insert(T object, BlueKey key) throws BlueDbException;

	public T get(BlueKey key) throws BlueDbException;

	public void update(BlueKey key, Updater<T> updater) throws BlueDbException;

	public void delete(BlueKey key) throws BlueDbException;

	public BlueQuery<T> query();
}
