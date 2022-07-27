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
public interface BlueTimeQuery<V extends Serializable> extends BlueQuery<V>, ReadBlueTimeQuery<V> {
	
	@Override
	BlueTimeQuery<V> where(Condition<V> condition);

	@Override
	BlueTimeQuery<V> where(BlueIndexCondition<?> indexCondition);
	
	@Override
	BlueTimeQuery<V> whereKeyIsIn(Set<BlueKey> keys);
	
	@Override
	BlueTimeQuery<V> whereKeyIsIn(BlueSimpleSet<BlueKey> keys);
	
	@Override
	BlueTimeQuery<V>  whereKeyIsActive();
	
	@Override
	BlueTimeQuery<V> whereKeyIsNotActive();
	
	@Override
	BlueTimeQuery<V> byStartTime();
	
	@Override
	BlueTimeQuery<V> beforeTime(long time);
	
	@Override
	BlueTimeQuery<V> beforeOrAtTime(long time);
	
	@Override
	BlueTimeQuery<V> afterTime(long time);
	
	@Override
	BlueTimeQuery<V> afterOrAtTime(long time);
	
	/**
	 * Executes the query and updates any matching values. Use this if you have to mutate a value in a
	 * way that will result in a new time key for that value. For example, if a value is no longer considered 
	 * active or the end time is being changed. The new key must be equivalent, meaning that the id and time/start-time
	 * are the same. Otherwise it will be considered a new key/value pair. ONLY SUPPORTED ON COLLECTION VERSIONS 2+.
	 * @param updater a function that mutates the values in the query results and returns the new key. The new key must be 
	 * equivalent, meaning that the id and time/start-time are the same. Otherwise it will be considered a new key/value pair.
	 * @throws BlueDbException if there are any problems, such as an exception thrown by the updater
	 */
	void updateKeyAndValue(TimeEntityUpdater<V> updater) throws BlueDbException;
	
	/**
	 * Executes the query and replaces any matching values. Use this if you have to replace a value in a
	 * way that will result in a new time key for that value. For example, if a value is no longer considered 
	 * active or the end time is being changed. The new key must be equivalent, meaning that the id and time/start-time 
	 * are the same. Otherwise it will be considered a new key/value pair. ONLY SUPPORTED ON COLLECTION VERSIONS 2+.
	 * @param mapper a function that returns a replacement key and value for values in the query results. The new key must be 
	 * equivalent, meaning that the id and time/start-time are the same. Otherwise it will be considered a new key/value pair.
	 * @throws BlueDbException if there are any problems, such as an exception thrown by the mapper
	 */
	void replaceKeyAndValue(TimeEntityMapper<V> mapper) throws BlueDbException;
}
