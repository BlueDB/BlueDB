package org.bluedb.api;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.bluedb.api.datastructures.BlueSimpleInMemorySet;
import org.bluedb.api.datastructures.BlueSimpleSet;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.conditions.BlueIndexCondition;
import org.bluedb.api.keys.BlueKey;

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
	 * Adds an index condition to the query before returning itself.
	 * @param indexCondition a filter function. Records that don't have indexed values 
	 * matching the condition will be excluded from the query.
	 * @return itself, with the index condition added to the query
	 */
	ReadBlueQuery<V> where(BlueIndexCondition<?> indexCondition);
	
	/**
	 * Adds a condition that results in only matching values for the given keys.
	 * BlueDB can optimize this search much better than if you used a standard
	 * {@link Condition} that checks your own set of keys. You can use the overloaded
	 * method if you want the same functionality without having to have all of the
	 * keys in memory at once.
	 * @param keys the keys that a value has to have in order to match the query.
	 * @return itself, with the condition added to the query
	 */
	default ReadBlueQuery<V> whereKeyIsIn(Set<BlueKey> keys) {
		return whereKeyIsIn(new BlueSimpleInMemorySet<BlueKey>(keys));
	}
	
	/**
	 * Adds a condition that results in only matching values for the given keys.
	 * BlueDB can optimize this search much better than if you used a standard
	 * {@link Condition} that checks your own set of keys. This method exists in
	 * order to allow you to have access to this functionality without having to
	 * store all of the keys in memory at once.
	 * @param keys the keys that a value has to have in order to match the query.
	 * @return itself, with the condition added to the query
	 */
	ReadBlueQuery<V> whereKeyIsIn(BlueSimpleSet<BlueKey> keys);

	/**
	 * Executes the query and returns the results as a list. Use getIterator if you don't want to load all matching 
	 * values into memory at once.
	 * @return the query results as a list
	 * @throws BlueDbException if the query fails
	 */
	List<V> getList() throws BlueDbException;

	/**
	 * Executes the query and returns the first result or Optional.empty if there are no results.
	 * @return the first result from the query or Optional.empty if there are no results.
	 * @throws BlueDbException if the query fails
	 */
	Optional<V> getFirst() throws BlueDbException;

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
	 * @throws BlueDbException if the query fails
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
	 * @param timeout the custom time this query will be automatically ended if inactive
	 * @param timeUnit the time unit for the timeout
	 * @return an iterator for the query results
	 * @throws BlueDbException if the query fails
	 */
	CloseableIterator<V> getIterator(long timeout, TimeUnit timeUnit) throws BlueDbException;

	/**
	 * Executes the query and returns the number of values matching the query
	 * @return the number of values matching the query
	 * @throws BlueDbException if the query fails
	 */
	public int count() throws BlueDbException;
}
