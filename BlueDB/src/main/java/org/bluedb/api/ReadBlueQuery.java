package org.bluedb.api;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.bluedb.api.exceptions.BlueDbException;

/**
 * Allows one to build and execute a query in a stream like way
 * @param <V> The value type of the collection being queried
 */
public interface ReadBlueQuery<V extends Serializable> {

	/**
	 * Adds a condition to the query before returning itself.
	 * @param condition a filter function to be applied to possible matching values
	 * @return itself, with the condition added to the query
	 */
	ReadBlueQuery<V> where(Condition<V> condition);

	/**
	 * Executes the query and returns the results as a list. Use getIterator if you don't want to load all matching 
	 * values into memory at once.
	 * @return the query results as a list
	 * @throws BlueDbException
	 */
	List<V> getList() throws BlueDbException;

	/**
	 * Begins executing the query and returns an iterator for processing the results. BlueDB will iterate over
	 * the collection on disk as you iterate over it in memory. This makes the iterator an extremely memory efficient
	 * way to read large collections. 
	 * 
	 * <br><br>
	 * 
	 * <b>Important: </b>Use within a try-with-resources statement and iterate through as quickly as possible
	 * in order to ensure that you don't block other BlueDB tasks. If you fail to call next for 15 seconds then
	 * the iterator will timeout and release resources.
	 * 
	 * @return an iterator for the query results
	 * @throws BlueDbException 
	 */
	CloseableIterator<V> getIterator() throws BlueDbException;

	/**
	 * Begins executing the query and returns an iterator for processing the results. BlueDB will iterate over
	 * the collection on disk as you iterate over it in memory. This makes the iterator an extremely memory efficient
	 * way to read large collections. 
	 * 
	 * <br><br>
	 * 
	 * <b>Important: </b>Use within a try-with-resources statement and iterate through as quickly as possible
	 * in order to ensure that you don't block other BlueDB tasks. If you fail to call next for the given timeout period then
	 * the iterator will timeout and release resources.
	 * 
	 * @return an iterator for the query results
	 * @throws BlueDbException 
	 */
	CloseableIterator<V> getIterator(long timeout, TimeUnit timeUnit) throws BlueDbException;

	/**
	 * Executes the query and returns the number of values matching the query
	 * @return the number of values matching the query
	 * @throws BlueDbException 
	 */
	public int count() throws BlueDbException;
}
