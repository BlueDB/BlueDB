package org.bluedb.api;

import java.io.Serializable;
import java.util.List;
import org.bluedb.api.exceptions.BlueDbException;

public interface BlueQuery<T extends Serializable> {

	/**
	 * Adds a condition to the query before returning itself.
	 * @param condition a filter function to be applied to possible matching values
	 * @return itself, with condition added to query
	 */
	BlueQuery<T> where(Condition<T> condition);

	/**
	 * For queries involving time frames, this adds a condition that the key time frame start after the time passed in with afterTime or afterOrAtTime.
	 * 
	 * Normally, it is only required that the time frame overlap the specified time frame.
	 * 
	 * This method has no effect when keys are not using time frames.
	 * @return itself, with condition added to query that the key start time be in the specified time interval
	 */
	BlueQuery<T> byStartTime();

	/**
	 * Adds (or tightens) an exclusive high limit to the key grouping number, then returns itself.
	 * @param time high limit (exclusive) for the keys to match the query
	 * @return itself, with additional condition that the grouping number of the key be lower than the time parameter
	 */
	BlueQuery<T> beforeTime(long time);

	/**
	 * Adds (or tightens) an inclusive high limit to the key grouping number, then returns itself.
	 * @param time high limit (inclusive) for the keys to match the query
	 * @return itself, with additional condition that the grouping number of the key be lower than or equal to the time parameter
	 */
	BlueQuery<T> beforeOrAtTime(long time);

	/**
	 * Adds (or tightens) an exclusive low limit to the key grouping number, then returns itself.
	 * @param time low limit (exclusive) for the keys to match the query
	 * @return itself, with additional condition that the grouping number of the key be higher than the time parameter
	 */
	BlueQuery<T> afterTime(long time);

	/**
	 * Adds (or tightens) an inclusive low limit to the key grouping number, then returns itself.
	 * @param time low limit (inclusive) for the keys to match the query
	 * @return itself, with additional condition that the grouping number of the key be higher than or equal to the time parameter
	 */
	BlueQuery<T> afterOrAtTime(long time);

	/**
	 * Get the query results as a list.
	 * @return the result of the query in a list
	 * @throws BlueDbException
	 */
	List<T> getList() throws BlueDbException;

	/**
	 * Get an iterator for query results.
	 * @return the result of the query as an iterator
	 * @throws BlueDbException 
	 */
	CloseableIterator<T> getIterator() throws BlueDbException;

	/**
	 * Deletes value stored at key, if it exists.
	 * @throws BlueDbException
	 */
	void delete() throws BlueDbException;

	/**
	 * Updates values in results from query, if it exists.
	 * @param updater function that mutates the values in the query results
	 * @throws BlueDbException if there is any problems, such as an exception thrown by the updater
	 */
	void update(Updater<T> updater) throws BlueDbException;

	/**
	 * Count the number of values matching the query.
	 * @return the count of results matching the query
	 * @throws BlueDbException 
	 */
	public int count() throws BlueDbException;
}
