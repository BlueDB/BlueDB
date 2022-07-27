package org.bluedb.api;

import java.io.Serializable;
import java.util.Set;

import org.bluedb.api.datastructures.BlueSimpleSet;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.conditions.BlueIndexCondition;
import org.bluedb.api.keys.BlueKey;

/**
 * Allows one to build and execute a query in a stream like way
 * @param <V> The value type of the collection being queried
 */
public interface BlueQuery<V extends Serializable> extends ReadBlueQuery<V> {
	
	@Override
	BlueQuery<V> where(Condition<V> condition);

	@Override
	BlueQuery<V> where(BlueIndexCondition<?> indexCondition);
	
	@Override
	BlueQuery<V> whereKeyIsIn(Set<BlueKey> keys);
	
	@Override
	BlueQuery<V> whereKeyIsIn(BlueSimpleSet<BlueKey> keys);
	
	/**
	 * Executes the query and deletes any matching values
	 * @throws BlueDbException if the query fails
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
