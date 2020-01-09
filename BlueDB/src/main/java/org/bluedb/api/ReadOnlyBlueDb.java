package org.bluedb.api;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.bluedb.api.exceptions.BlueDbException;

/**
 * {@link ReadOnlyBlueDb} is a set of {@link ReadOnlyBlueCollection} instances. Each collection must have a different name and can be of 
 * a different type.
 */
public interface ReadOnlyBlueDb {
	
	/**
	 * Gets an existing {@link ReadOnlyBlueCollection}
	 * 
	 * @param <V> the object type of values to be serialized into the collection
	 * 
	 * @param name The unique name of the collection
	 * @param valueType the class type of the values stored in the collection
	 * 
	 * @return existing {@link ReadOnlyBlueCollection} if it exists, else null
	 * 
	 * @throws BlueDbException if any issues encountered, such as the existing collection having a different key type
	 */
	public <V extends Serializable> ReadOnlyBlueCollection<V> getCollection(String name, Class<V> valueType) throws BlueDbException;
	
	/**
	 * Gets an existing {@link ReadOnlyBlueCollection}
	 * 
	 * @param <V> the object type of values to be serialized into the collection
	 * 
	 * @param name The unique name of the collection
	 * @param valueType the class type of the values stored in the collection
	 * 
	 * @return existing {@link ReadOnlyBlueCollection} if it exists, else null
	 * 
	 * @throws BlueDbException if any issues encountered, such as the existing collection having a different key type
	 */
	public <V extends Serializable> ReadOnlyBlueTimeCollection<V> getTimeCollection(String name, Class<V> valueType) throws BlueDbException;

	/*
	 * The backup method is not allowed on read only instances of BlueDB at this time. In order for the backup to work properly
	 * the change log has to not be cleaned up while the backup is running. We'd like to support the case where a single read/write 
	 * BlueDB instance is running while other read only instances of BlueDB are running. In this scenario there is no way for the
	 * read only instance of BlueDB to signal the read/write instance that a backup is in progress. 
	 */

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
