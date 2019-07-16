package org.bluedb.api;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.ValueKey;

public interface BlueCollection<V extends Serializable> {

	public <K extends ValueKey> BlueIndex<K, V> createIndex(String name, Class<K> keyType, KeyExtractor<K, V> keyExtractor) throws BlueDbException;

	public <K extends ValueKey> BlueIndex<K, V> getIndex(String name, Class<K> keyType) throws BlueDbException;

	public boolean contains(BlueKey key) throws BlueDbException;

	public void insert(BlueKey key, V value) throws BlueDbException;

	public void batchUpsert(Map<BlueKey, V> values) throws BlueDbException;

	public void batchDelete(Collection<BlueKey> keys) throws BlueDbException;

	public V get(BlueKey key) throws BlueDbException;

	public void replace(BlueKey key, Mapper<V> updater) throws BlueDbException;

	public void update(BlueKey key, Updater<V> updater) throws BlueDbException;

	public void delete(BlueKey key) throws BlueDbException;

	public BlueQuery<V> query();

	public BlueKey getLastKey() throws BlueDbException;
}
