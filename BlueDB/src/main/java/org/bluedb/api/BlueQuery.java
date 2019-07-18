package org.bluedb.api;

import java.io.Serializable;
import java.util.List;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;

public interface BlueQuery<V extends Serializable> {

	/**
	 * Adds a condition to the query before returning itself.
	 * @param condition a filter function to be applied to possible matching values
	 * @return itself, with the condition added to the query
	 */
	BlueQuery<V> where(Condition<V> condition);

	/**
	 * For queries on a collection with a key type of {@link TimeFrameKey}, this adds a condition that the value starts in the 
	 * queried interval. Normally, it is only required that the value's time interval overlaps the queried time interval.
	 * <br><br>
	 * This can be very useful if you want to map reduce the results. Instead of making one query for the entire time interval you
	 * can split it into multiple time intervals to be queried and processed individually. By using this condition on all but the 
	 * first query you can guarantee that each value will only be processed once.
	 * <br><br>
	 * This condition only has an effect on collections with a key type of {@link TimeFrameKey}
	 * 
	 * @return itself, with the condition added to the query that the key start time be in the queried time interval
	 */
	BlueQuery<V> byStartTime();

	/**
	 * Adds an exclusive max time for the queried time interval. For a {@link TimeFrameKey} this means that it starts 
	 * before the specified time. For a {@link TimeKey} it means that it is before the specified time. It doesn't make 
	 * sense to use it with other key types.
	 * 
	 * @param time an exclusive max time (millis since epoch) for the queried time interval
	 * @return itself, with an exclusive max time for the queried time interval
	 */
	BlueQuery<V> beforeTime(long time);

	/**
	 * Adds an inclusive max time for the queried time interval. For a {@link TimeFrameKey} this means that it starts 
	 * before or at the specified time. For a {@link TimeKey} it means that it is before or at the specified time. It doesn't 
	 * make sense to use it with other key types.
	 * 
	 * @param time an inclusive max time (millis since epoch) for the queried time interval
	 * @return itself, with an inclusive max time for the queried time interval
	 */
	BlueQuery<V> beforeOrAtTime(long time);

	/**
	 * Adds an exclusive min time for the queried time interval. For a {@link TimeFrameKey} this means that it ends 
	 * after the specified time. For a {@link TimeKey} it means that it is after the specified time. It doesn't make 
	 * sense to use it with other key types.
	 * 
	 * @param time an exclusive min time (millis since epoch) for the queried time interval
	 * @return itself, with an exclusive min time for the queried time interval
	 */
	BlueQuery<V> afterTime(long time);

	/**
	 * Adds an inclusive min time for the queried time interval. For a {@link TimeFrameKey} this means that it ends 
	 * after or at the specified time. For a {@link TimeKey} it means that it is after or at the specified time. It doesn't 
	 * make sense to use it with other key types.
	 * 
	 * @param time an inclusive min time (millis since epoch) for the queried time interval
	 * @return itself, with an exclusive min time for the queried time interval
	 */
	BlueQuery<V> afterOrAtTime(long time);

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

	/**
	 * Executes the query and returns the number of values matching the query
	 * @return the number of values matching the query
	 * @throws BlueDbException 
	 */
	public int count() throws BlueDbException;
}
