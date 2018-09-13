package io.bluedb.api;

import java.io.Serializable;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.ValueKey;

public interface BlueCollection<T extends Serializable> {

	public <I extends ValueKey> BlueIndex<I, T> createIndex(String name, Class<I> keyType, KeyExtractor<I, T> keyExtractor) throws BlueDbException;

	public <I extends ValueKey> BlueIndex<I, T> getIndex(String name, Class<I> keyType) throws BlueDbException;

	public boolean contains(BlueKey key) throws BlueDbException;

	public void insert(BlueKey key, T object) throws BlueDbException;

	public T get(BlueKey key) throws BlueDbException;

	public void update(BlueKey key, Updater<T> updater) throws BlueDbException;

	public void delete(BlueKey key) throws BlueDbException;

	public BlueQuery<T> query();

	public BlueKey getLastKey() throws BlueDbException;
}
