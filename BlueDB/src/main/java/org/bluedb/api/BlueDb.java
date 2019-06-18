package org.bluedb.api;

import java.io.Serializable;
import java.nio.file.Path;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;

/**
 * A BlueDb is a set of BlueCollections.  Each collection must have a different name and can be of a different type.
 * 
 * There can only be one BlueCollection instance per collection name.
 */
public interface BlueDb {
	
	/**
	 * Creates a collection, or returns the existing one of the same name, if one exists.
	 * @param name name of collection, only one collection per name
	 * @param keyType class of key
	 * @param valueType class of values stored in the collection
	 * @param additionalClassesToRegister classes to optimizer for in the serializer (should be classes that will be stored in collection keys or values)
	 * @return a new BlueCollection, or the existing one of the same name, if it exists
	 * @throws BlueDbException if any problems are encountered, such as collection already existing with a different type 
	 */
	public <T extends Serializable> BlueCollection<T> initializeCollection(String name, Class<? extends BlueKey> keyType, Class<T> valueType, @SuppressWarnings("unchecked") Class<? extends Serializable>... additionalClassesToRegister) throws BlueDbException;

	/**
	 * Create CollectionBuilder for this BlueDb
	 * @param name name of collection
	 * @param keyType class of keys used in collection
	 * @param valueType class of values stored in collection
	 * @return new CollectionBuilder
	 */
	public <T extends Serializable, K extends BlueKey> BlueCollectionBuilder<T> collectionBuilder(String name, Class<K> keyType, Class<T> valueType);

	/**
	 * Get existing BlueCollection.
	 * @param name name of collection
	 * @param valueType class of values stored in collection
	 * @return existing BlueCollection, if it exists, else null
	 * @throws BlueDbException if any issues encountered, such as existing collection has a different key type
	 */
	public <T extends Serializable> BlueCollection<T> getCollection(String name, Class<T> valueType) throws BlueDbException;

	/**
	 * Create a backup of the entire database.
	 * @param path where backup will be created
	 * @throws BlueDbException 
	 * @throws BlueDbException if any issues encountered, such as file system problems
	 */
	public void backup(Path path) throws BlueDbException;

	/**
	 * Shutdown BlueDb and all related threads, in an orderly manner
	 */
	public void shutdown();
}
