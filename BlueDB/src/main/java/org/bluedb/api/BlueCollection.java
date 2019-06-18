package org.bluedb.api;

import java.io.Serializable;
import java.util.Map;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.ValueKey;

/**
 * A BlueCollection is a persisted map of keys (of type BlueKey) to values of object type T.
 * 
 * A collection has a name (to distinguish between collections in a BlueDb instance), a key type, and a value type.
 * @param <T> object type of values to be serialized into collection
 */
public interface BlueCollection<T extends Serializable> {

	/**
	 * Creates (or returns existing) BlueIndex that maps objects of type keyType to values in the collection.
	 * @param name index (one index per name per each collection)
	 * @param keyType the type of each key which is used to lookup a value using the index (this must match the keyType of any existing index with the same name)
	 * @param keyExtractor a function that maps a value to the keys by which the value should be indexed
	 * @return a BlueIndex (existing index if it exists, otherwise a newly created index)
	 * @throws BlueDbException if index doesn't exist or has a different keyType
	 */
	public <I extends ValueKey> BlueIndex<I, T> createIndex(String name, Class<I> keyType, KeyExtractor<I, T> keyExtractor) throws BlueDbException;

	/**
	 * Returns existing BlueIndex that maps objects of type keyType to values in the collection.
	 * @param name index (one index per name per each collection)
	 * @param keyType the type of each key which is used to lookup a value using the index (this must match the keyType of any existing index with the same name)
	 * @return a BlueIndex with the same name if it exists and has the same keyType
	 * @throws BlueDbException if index doesn't exist or has a different keyType
	 */
	public <I extends ValueKey> BlueIndex<I, T> getIndex(String name, Class<I> keyType) throws BlueDbException;

	/**
	 * Returns true if the collection contains a value at key.
	 * @param key key that may or may not be in collection
	 * @return true if the collection contains a value at the key, else false
	 * @throws BlueDbException if type of key is not the type specified when the index was created
	 */
	public boolean contains(BlueKey key) throws BlueDbException;

	/**
	 * Inserts the value at the key.
	 * @param key key where value should be saved (key is of type keyType specified when getIndex or createIndex was called)
	 * @param value value to be saved at the key
	 * @throws BlueDbException if type of key is not the type specified when the index was created
	 */
	public void insert(BlueKey key, T value) throws BlueDbException;

	/**
	 * Inserts or replaces values at the corresponding keys
	 * @param values a map of keys and the values that should be stored at each key
	 * @throws BlueDbException if type of key is not the type specified when the index was created
	 */
	public void batchUpsert(Map<BlueKey, T> values) throws BlueDbException;

	/**
	 * Returns the value stored at key.
	 * @param key key where the value is stored
	 * @return value stored at key, or null
	 * @throws BlueDbException if any of the keys are not the same type specified when getIndex or createIndex was called
	 */
	public T get(BlueKey key) throws BlueDbException;

	/**
	 * Mutates value stored at key by passing it through the updater.
	 * @param key key where the value is stored
	 * @param updater function that mutates the value stored at key
	 * @throws BlueDbException if type of key is not the type specified when the index was created or if updater throws exception
	 */
	public void update(BlueKey key, Updater<T> updater) throws BlueDbException;

	/**
	 * Deletes value stored at key, if any.
	 * @param key key where the value is stored
	 * @throws BlueDbException if type of key is not the type specified when the index was created
	 */
	public void delete(BlueKey key) throws BlueDbException;

	/**
	 * Creates, but does not run, a BlueQuery.
	 * @return a BlueQuery
	 */
	public BlueQuery<T> query();

	/**
	 * Returns the saved key with the highest grouping number.
	 * @return the key with the highest grouping number or null if collection is empty
	 */
	public BlueKey getLastKey();
}
