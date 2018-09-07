package io.bluedb.api;

import java.io.Serializable;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;

public interface BlueCollection<T extends Serializable> {

	public <K extends BlueKey> BlueIndex<K, T> createIndex(String name, Class<K> keyType, KeyExtractor<K, T> keyExtractor) throws BlueDbException;

	public <K extends BlueKey> BlueIndex<K, T> getIndex(String name, Class<K> keyType) throws BlueDbException;

	public boolean contains(BlueKey key) throws BlueDbException;

	public void insert(BlueKey key, T object) throws BlueDbException;

	public T get(BlueKey key) throws BlueDbException;

	public void update(BlueKey key, Updater<T> updater) throws BlueDbException;

	public void delete(BlueKey key) throws BlueDbException;

	public BlueQuery<T> query();

	public BlueKey getLastKey() throws BlueDbException;
}
