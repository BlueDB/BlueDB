package org.bluedb.api;

import java.io.Serializable;
import java.nio.file.Path;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;

/**
 * {@link BlueDb} is a set of {@link BlueCollection} instances. Each collection must have a different name and can be of 
 * a different type.
 */
public interface BlueDb extends ReadableBlueDb {
	
	/**
	 * Creates a {@link BlueCollectionBuilder} object for this {@link BlueDb}
	 * 
	 * @param <K> the key type of the collection
	 * @param <V> the object type of values to be serialized into the collection
	 * 
	 * @param name The unique name of the collection
	 * @param keyType the class type of the collection's keys
	 * @param valueType the class type of the values stored in the collection
	 * @return a new {@link BlueCollectionBuilder} object for this {@link BlueDb}
	 */
	public <K extends BlueKey, V extends Serializable> BlueCollectionBuilder<K, V> getCollectionBuilder(String name, Class<K> keyType, Class<V> valueType);
	
	/**
	 * Creates a {@link BlueTimeCollectionBuilder} object for this {@link BlueDb}
	 * 
	 * @param <K> the key type of the collection
	 * @param <V> the object type of values to be serialized into the collection
	 * 
	 * @param name The unique name of the collection
	 * @param keyType the class type of the collection's keys
	 * @param valueType the class type of the values stored in the collection
	 * @return a new {@link BlueCollectionBuilder} object for this {@link BlueDb}
	 */
	public <K extends BlueKey, V extends Serializable> BlueTimeCollectionBuilder<K, V> getTimeCollectionBuilder(String name, Class<K> keyType, Class<V> valueType);

	/**
	 * Gets an existing {@link BlueCollection}
	 * 
	 * @param <V> the object type of values to be serialized into the collection
	 * 
	 * @param name The unique name of the collection
	 * @param valueType the class type of the values stored in the collection
	 * 
	 * @return existing {@link BlueCollection} if it exists, else null
	 * 
	 * @throws BlueDbException if any issues encountered, such as the existing collection having a different key type
	 */
	@Override
	public <V extends Serializable> BlueCollection<V> getCollection(String name, Class<V> valueType) throws BlueDbException;
	
	/**
	 * Gets an existing {@link BlueTimeCollection}
	 * 
	 * @param <V> the object type of values to be serialized into the collection
	 * 
	 * @param name The unique name of the collection
	 * @param valueType the class type of the values stored in the collection
	 * 
	 * @return existing {@link BlueTimeCollection} if it exists, else null
	 * 
	 * @throws BlueDbException if any issues encountered, such as the existing collection having a different key type
	 */
	public <V extends Serializable> BlueTimeCollection<V> getTimeCollection(String name, Class<V> valueType) throws BlueDbException;
	
	/**
	 * Creates a backup of the entire database
	 * @param path where the backup will be created
	 * @throws BlueDbException if any issues encountered, such as file system problems
	 */
	public void backup(Path path) throws BlueDbException;
	
	/**
	 * Creates a partial backup of the database. All non-time based data will be included. Time based data will only be included
	 * if it overlaps with the time frame that is passed in via the startTime and endTime arguments.
	 * @param path where the backup will be created
	 * @param startTime The start of the timeframe that should be included in the backup
	 * @param endTime The end of the timeframe that should be included in the backup
	 * @throws BlueDbException if any issues encountered, such as file system problems
	 */
	public void backupTimeFrame(Path path, long startTime, long endTime) throws BlueDbException;

}
