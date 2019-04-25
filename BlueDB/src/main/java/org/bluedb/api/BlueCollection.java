package org.bluedb.api;

import java.io.Serializable;
import java.util.Map;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.ValueKey;

public interface BlueCollection<T extends Serializable> {

	public <I extends ValueKey> BlueIndex<I, T> createIndex(String name, Class<I> keyType, KeyExtractor<I, T> keyExtractor) throws BlueDbException;

	public <I extends ValueKey> BlueIndex<I, T> getIndex(String name, Class<I> keyType) throws BlueDbException;

	public boolean contains(BlueKey key) throws BlueDbException;

	public void insert(BlueKey key, T value) throws BlueDbException;

	public void batchUpsert(Map<BlueKey, T> values) throws BlueDbException;

	public T get(BlueKey key) throws BlueDbException;

	public void update(BlueKey key, Updater<T> updater) throws BlueDbException;

	public void delete(BlueKey key) throws BlueDbException;

	public BlueQuery<T> query();

	public BlueKey getLastKey() throws BlueDbException;
}
