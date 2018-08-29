package io.bluedb.api;

import java.io.Serializable;
import java.util.List;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;

public interface BlueIndex<K extends BlueKey, T extends Serializable> {
	public void add(BlueKey key, T newItem) throws BlueDbException;
	public void remove(K key, T newItem) throws BlueDbException;
	public List<T> get(K key) throws BlueDbException;
}
