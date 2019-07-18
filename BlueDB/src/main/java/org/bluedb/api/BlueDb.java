package org.bluedb.api;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;

/**
 * {@link BlueDb} is a set of {@link BlueCollection} instances. Each collection must have a different name and can be of 
 * a different type.
 */
public interface BlueDb {
	
	/**
	 * Creates a {@link BlueCollection}, or returns the existing one of the same name, if one exists
	 * 
	 * @param <V> the object type of values to be serialized into the collection
	 * 
	 * @param name the unique name of the collection
	 * @param keyType the class type of the collection's keys
	 * @param valueType the class type of the values stored in the collection
	 * @param additionalClassesToRegister classes to optimize for in the serializer (should be classes that will be stored in collection keys or values)
	 * 
	 * @return a new BlueCollection, or the existing one if it exists
	 * 
	 * @throws BlueDbException if any problems are encountered, such as the collection already existing with a different type 
	 */
	public <V extends Serializable> BlueCollection<V> initializeCollection(String name, Class<? extends BlueKey> keyType, Class<V> valueType, @SuppressWarnings("unchecked") Class<? extends Serializable>... additionalClassesToRegister) throws BlueDbException;

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
	public <K extends BlueKey, V extends Serializable> BlueCollectionBuilder<K, V> collectionBuilder(String name, Class<K> keyType, Class<V> valueType);

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
	public <V extends Serializable> BlueCollection<V> getCollection(String name, Class<V> valueType) throws BlueDbException;

	/**
	 * Creates a backup of the entire database
	 * @param path where the backup will be created
	 * @throws BlueDbException if any issues encountered, such as file system problems
	 */
	public void backup(Path path) throws BlueDbException;

	/**
	 * Initiates an orderly shutdown in which previously submitted tasks are executed, but no new tasks will be accepted.<br><br>
	 * 
	 * This method does not wait for previously submitted tasks to complete execution. Use awaitTermination to do that.
	 */
	public void shutdown();

	/**
	 * Attempts to stop all actively executing tasks and halts the processing of waiting tasks.<br><br>
	 * 
	 * This method does not wait for actively executing tasks to terminate. Use awaitTermination to do that.
	 */
	public void shutdownNow();
	
	/**
	 * Blocks until all tasks have completed execution after a shutdown request, or the timeout occurs, or the current 
	 * thread is interrupted, whichever happens first.
	 * @param timeout the maximum time to wait
	 * @param timeUnit the time unit of the timeout argument
	 * @return true if bluedb terminated and false if the timeout elapsed before termination
	 * @throws BlueDbException if any issues are encountered, such as being interrupted
	 */
	public boolean awaitTermination(long timeout, TimeUnit timeUnit) throws BlueDbException;
}
