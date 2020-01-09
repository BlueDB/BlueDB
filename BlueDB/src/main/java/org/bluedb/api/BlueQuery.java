package org.bluedb.api;

import java.io.Serializable;

import org.bluedb.api.exceptions.BlueDbException;

/**
 * Allows one to build and execute a query in a stream like way
 * @param <V> The value type of the collection being queried
 */
public interface BlueQuery<V extends Serializable> extends ReadOnlyBlueQuery<V> {
	
	@Override
	BlueQuery<V> where(Condition<V> condition);
	
	/**
	 * Executes the query and deletes any matching values
	 * @throws BlueDbException
	 */
	void delete() throws BlueDbException;

	/**
	 * Executes the query and updates any matching values
	 * @param updater a function that mutates the values in the query results
	 * @throws BlueDbException if there are any problems, such as an exception thrown by the updater
	 */
	void update(Updater<V> updater) throws BlueDbException;
	
	/**
	 * Executes the query and replaces any matching values
	 * @param mapper a function that replaces the values in the query results
	 * @throws BlueDbException if there are any problems, such as an exception thrown by the mapper
	 */
	void replace(Mapper<V> mapper) throws BlueDbException;

}
