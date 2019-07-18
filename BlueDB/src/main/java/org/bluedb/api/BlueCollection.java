package org.bluedb.api;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.IntegerIndexKeyExtractor;
import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.index.LongIndexKeyExtractor;
import org.bluedb.api.index.StringIndexKeyExtractor;
import org.bluedb.api.index.UUIDIndexKeyExtractor;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.UUIDKey;
import org.bluedb.api.keys.ValueKey;

/**
 * A BlueCollection represents a persisted map of keys (of type {@link BlueKey}) to values of object type V.
 * 
 * A collection has a name (to distinguish between collections in a {@link BlueDb} instance), a key type, and a value type.
 * @param <V> the object type of values to be serialized into the collection
 */
public interface BlueCollection<V extends Serializable> {

	/**
	 * Creates (or returns existing) {@link BlueIndex} that maps objects of type {@link ValueKey} ({@link UUIDKey}, {@link StringKey}, {@link IntegerKey}, {@link LongKey}) to values in the collection.
	 * 
	 * @param <K> - the key type of the index or the type of data that the collection is being indexed on. It must be a concretion of 
	 * {@link ValueKey} ({@link UUIDKey}, {@link StringKey}, {@link LongKey}, or {@link IntegerKey}).
	 * 
	 * @param name - index name (one index per name per each collection)
	 * @param keyType - the type of each key which is used to lookup a value using the index (this must match the keyType of any existing index with the same name)
	 * @param keyExtractor - a function that maps a value to the keys by which the value should be indexed. The appropriate subclass can be used and would be more
	 * simple to implement. These include {@link UUIDIndexKeyExtractor}, {@link StringIndexKeyExtractor}, {@link IntegerIndexKeyExtractor}, and {@link LongIndexKeyExtractor}
	 * 
	 * @return a {@link BlueIndex} object (existing index if it exists, otherwise a newly created index)
	 * 
	 * @throws BlueDbException if the index exists but is not compatible with these types
	 */
	public <K extends ValueKey> BlueIndex<K, V> createIndex(String name, Class<K> keyType, KeyExtractor<K, V> keyExtractor) throws BlueDbException;

	/**
	 * Returns existing BlueIndex that maps objects of type keyType to values in the collection.
	 * 
	 * @param <K> - the key type of the index or the type of data that the collection is being indexed on. It must be a concretion of 
	 * {@link ValueKey} ({@link UUIDKey}, {@link StringKey}, {@link LongKey}, or {@link IntegerKey}).
	 * 
	 * @param name - index name (one index per name per each collection)
	 * @param keyType - the type of each key which is used to lookup a value using the index (this must match the keyType of any existing index with the same name)
	 * 
	 * @return a {@link BlueIndex} with the same name if it exists and has the same keyType
	 * 
	 * @throws BlueDbException if index doesn't exist or has a different keyType
	 */
	public <K extends ValueKey> BlueIndex<K, V> getIndex(String name, Class<K> keyType) throws BlueDbException;

	/**
	 * Returns true if the collection contains a value for the given key.
	 * @param key - the key that may or may not be in collection
	 * @return true if the collection contains a value for the given key, else false
	 * @throws BlueDbException if the type of key was not the type specified when the collection was created
	 */
	public boolean contains(BlueKey key) throws BlueDbException;

	/**
	 * Inserts the given key value pair
	 * @param key - key where value should be saved (must match the keyType specified when the collection was created)
	 * @param value - value to be saved for the key
	 * @throws BlueDbException if the key type is not the type specified when the collection was created
	 */
	public void insert(BlueKey key, V value) throws BlueDbException;
	
	/**
	 * Inserts or replaces the given key value pairs. Batch methods are much more efficient than calling non-batch methods many times. 
	 * @param values - the key value pairs to insert. Key types must match the keyType specified when the collection was created.
	 * @throws BlueDbException if the key types do not match the type specified when the collection was created
	 */
	public void batchUpsert(Map<BlueKey, V> values) throws BlueDbException;

	/**
	 * Returns the value to which the specified key is mapped, or null if this map contains no mapping for the key
	 * @param key - the key for the desired value
	 * @return the value to which the specified key is mapped, or null if this map contains no mapping for the key
	 * @throws BlueDbException if the key is not the same type specified when the collection was created
	 */
	public V get(BlueKey key) throws BlueDbException;

	/**
	 * Mutates the value for the given key by passing it to the given updater
	 * @param key - The key for the value which will be updated
	 * @param updater - a function that mutates the value to which the specified key is mapped
	 * @throws BlueDbException if type of key is not the type specified when the collection was created or if updater throws an exception
	 */
	public void update(BlueKey key, Updater<V> updater) throws BlueDbException;
	
	/**
	 * Replaces the value for the given key by passing it to the given updater
	 * @param key - the key for the value which will be replaced
	 * @param updater - a function that returns the value that should replace the value to which the specified key is mapped
	 * @throws BlueDbException if type of key is not the type specified when the collection was created or if updater throws an exception
	 */
	public void replace(BlueKey key, Mapper<V> updater) throws BlueDbException;

	/**
	 * Deletes the value for the given key
	 * @param key - the key for the value which will be deleted
	 * @throws BlueDbException if type of key is not the type specified when the collection was created
	 */
	public void delete(BlueKey key) throws BlueDbException;

	/**
	 * Deletes the values for the given keys. Batch methods are much more efficient than calling non-batch methods many times.
	 * @param keys - the keys for the values which will be deleted
	 * @throws BlueDbException if type of key is not the type specified when the collection was created
	 */
	public void batchDelete(Collection<BlueKey> keys) throws BlueDbException;

	/**
	 * Creates a {@link BlueQuery} object which can be used to build and execute a query against this collection.
	 * @return a {@link BlueQuery} object which can be used to build and execute a query against this collection.
	 */
	public BlueQuery<V> query();

	/**
	 * Returns the key with the highest grouping number that exists in this collection
	 * @return the key with the highest grouping number that exists in this collection
	 */
	public BlueKey getLastKey();
}
