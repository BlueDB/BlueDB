package org.bluedb.api;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

import org.bluedb.api.datastructures.BlueKeyValuePair;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.TimeKey;

/**
 * A BlueTimeCollection represents a persisted map of keys (of type {@link TimeKey}) to values of object type V.
 * 
 * A collection has a name (to distinguish between collections in a {@link BlueDb} instance), a key type, and a value type.
 * @param <V> the object type of values to be serialized into the collection
 */
public interface BlueTimeCollection<V extends Serializable> extends BlueCollection<V>, ReadableBlueTimeCollection<V> {

	/**
	 * Creates a {@link BlueTimeQuery} object which can be used to build and execute a query against this collection.
	 * @return a {@link BlueTimeQuery} object which can be used to build and execute a query against this collection.
	 */
	@Override
	public BlueTimeQuery<V> query();
	
	/**
	 * Inserts or replaces the given key value pairs. Batch methods are much more efficient than calling non-batch methods many times.
	 * Using an iterator instead of a map allows you to provide entries without storing all of them in memory at one time. 
	 * If this is a version 2 time collection then you can specify a new but equivalent key to replace the old one. You 
	 * would use this to end an active record or to change the end time on a record without deleting and re-adding that 
	 * record.
	 * @param values the key value pairs to insert. Key types must match the keyType specified when the collection was created.
	 * @throws BlueDbException if the key types do not match the type specified when the collection was created
	 */
	public void batchUpsertKeysAndValues(Map<BlueKey, V> values) throws BlueDbException;
	
	/**
	 * Inserts or replaces the given key value pairs. Batch methods are much more efficient than calling non-batch 
	 * methods many times. If this is a version 2 time collection then you can specify a new but equivalent key to 
	 * replace the old one. You would use this to end an active record or to change the end time on a record without 
	 * deleting and re-adding that record.
	 * @param keyValuePairs the key value pairs to insert. Key types must match the keyType specified when the 
	 * collection was created.
	 * @throws BlueDbException if the key types do not match the type specified when the collection was created
	 */
	public void batchUpsertKeysAndValues(Iterator<BlueKeyValuePair<V>> keyValuePairs) throws BlueDbException;

	/**
	 * Mutates the value for the given key by passing it to the given updater. If this is a version 2+ time collection then you can
	 * specify a new but equivalent key to replace the old one. You would use this to end an active record or to change the end
	 * time on a record without deleting and re-adding that record. 
	 * @param key The key for the value which will be updated
	 * @param updater a function that mutates the value to which the specified key is mapped
	 * @throws BlueDbException if type of key is not the type specified when the collection was created or if updater throws an exception
	 */
	public void updateKeyAndValue(TimeKey key, Updater<V> updater) throws BlueDbException;
	
	/**
	 * Replaces the value for the given key by passing it to the given updater. If this is a version 2+ time collection then you can
	 * specify a new but equivalent key to replace the old one. You would use this to end an active record or to change the end
	 * time on a record without deleting and re-adding that record.
	 * @param key the key for the value which will be replaced
	 * @param updater a function that returns the value that should replace the value to which the specified key is mapped
	 * @throws BlueDbException if type of key is not the type specified when the collection was created or if updater throws an exception
	 */
	public void replaceKeyAndValue(TimeKey key, Mapper<V> updater) throws BlueDbException;

}
