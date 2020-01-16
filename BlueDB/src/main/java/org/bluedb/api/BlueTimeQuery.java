package org.bluedb.api;

import java.io.Serializable;

/**
 * Allows one to build and execute a query in a stream like way
 * @param <V> The value type of the collection being queried
 */
public interface BlueTimeQuery<V extends Serializable> extends BlueQuery<V>, ReadBlueTimeQuery<V> {
	
	@Override
	BlueTimeQuery<V> where(Condition<V> condition);
	
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
}
