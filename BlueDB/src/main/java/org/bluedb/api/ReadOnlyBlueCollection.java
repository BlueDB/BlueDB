package org.bluedb.api;

import java.io.Serializable;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.UUIDKey;
import org.bluedb.api.keys.ValueKey;

/**
 * A ReadOnlyBlueCollection represents a persisted map of keys (of type {@link BlueKey}) to values of object type V.
 * A collection has a name (to distinguish between collections in a {@link ReadOnlyBlueDb} instance), a key type, and a value type.
 * This collection will not edit any files or allow any updates to the data inside it.
 * @param <V> the object type of values to be serialized into the collection
 */
public interface ReadOnlyBlueCollection<V extends Serializable> {

	/**
	 * Returns existing BlueIndex that maps objects of type keyType to values in the collection.
	 * 
	 * @param <K> the key type of the index or the type of data that the collection is being indexed on. It must be a concretion of 
	 * {@link ValueKey} ({@link UUIDKey}, {@link StringKey}, {@link LongKey}, or {@link IntegerKey}).
	 * 
	 * @param name index name (one index per name per each collection)
	 * @param keyType the type of each key which is used to lookup a value using the index (this must match the keyType of any existing index with the same name)
	 * 
	 * @return a {@link BlueIndex} with the same name if it exists and has the same keyType
	 * 
	 * @throws BlueDbException if index doesn't exist or has a different keyType
	 */
	public <K extends ValueKey> BlueIndex<K, V> getIndex(String name, Class<K> keyType) throws BlueDbException;

	/**
	 * Returns true if the collection contains a value for the given key.
	 * @param key the key that may or may not be in collection
	 * @return true if the collection contains a value for the given key, else false
	 * @throws BlueDbException if the type of key was not the type specified when the collection was created
	 */
	public boolean contains(BlueKey key) throws BlueDbException;

	/**
	 * Returns the value to which the specified key is mapped, or null if this map contains no mapping for the key
	 * @param key the key for the desired value
	 * @return the value to which the specified key is mapped, or null if this map contains no mapping for the key
	 * @throws BlueDbException if the key is not the same type specified when the collection was created
	 */
	public V get(BlueKey key) throws BlueDbException;

	/**
	 * Returns the key with the highest grouping number that exists in this collection
	 * @return the key with the highest grouping number that exists in this collection
	 */
	public BlueKey getLastKey();

	/**
	 * Creates a {@link ReadOnlyBlueQuery} object which can be used to build and execute a query against this collection.
	 * @return a {@link ReadOnlyBlueQuery} object which can be used to build and execute a query against this collection.
	 */
	public ReadOnlyBlueQuery<V> query();
}
