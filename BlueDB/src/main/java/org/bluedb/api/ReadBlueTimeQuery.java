package org.bluedb.api;

import java.io.Serializable;
import java.util.Set;

import org.bluedb.api.datastructures.BlueSimpleSet;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;

/**
 * Allows one to build and execute a query on a time based collection in a stream like way
 * @param <V> The value type of the collection being queried
 */
public interface ReadBlueTimeQuery<V extends Serializable> extends ReadBlueQuery<V> {
	
	@Override
	ReadBlueTimeQuery<V> where(Condition<V> condition);
	
	@Override
	ReadBlueTimeQuery<V> whereKeyIsIn(Set<BlueKey> keys);
	
	@Override
	ReadBlueTimeQuery<V> whereKeyIsIn(BlueSimpleSet<BlueKey> keys);
	
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
	ReadBlueTimeQuery<V> byStartTime();

	/**
	 * Adds an exclusive max time for the queried time interval. For a {@link TimeFrameKey} this means that it starts 
	 * before the specified time. For a {@link TimeKey} it means that it is before the specified time. It doesn't make 
	 * sense to use it with other key types.
	 * 
	 * @param time an exclusive max time (millis since epoch) for the queried time interval
	 * @return itself, with an exclusive max time for the queried time interval
	 */
	ReadBlueTimeQuery<V> beforeTime(long time);

	/**
	 * Adds an inclusive max time for the queried time interval. For a {@link TimeFrameKey} this means that it starts 
	 * before or at the specified time. For a {@link TimeKey} it means that it is before or at the specified time. It doesn't 
	 * make sense to use it with other key types.
	 * 
	 * @param time an inclusive max time (millis since epoch) for the queried time interval
	 * @return itself, with an inclusive max time for the queried time interval
	 */
	ReadBlueTimeQuery<V> beforeOrAtTime(long time);

	/**
	 * Adds an exclusive min time for the queried time interval. For a {@link TimeFrameKey} this means that it ends 
	 * after the specified time. For a {@link TimeKey} it means that it is after the specified time. It doesn't make 
	 * sense to use it with other key types.
	 * 
	 * @param time an exclusive min time (millis since epoch) for the queried time interval
	 * @return itself, with an exclusive min time for the queried time interval
	 */
	ReadBlueTimeQuery<V> afterTime(long time);

	/**
	 * Adds an inclusive min time for the queried time interval. For a {@link TimeFrameKey} this means that it ends 
	 * after or at the specified time. For a {@link TimeKey} it means that it is after or at the specified time. It doesn't 
	 * make sense to use it with other key types.
	 * 
	 * @param time an inclusive min time (millis since epoch) for the queried time interval
	 * @return itself, with an exclusive min time for the queried time interval
	 */
	ReadBlueTimeQuery<V> afterOrAtTime(long time);
	
}
