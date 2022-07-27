package org.bluedb.api;

import java.io.Serializable;

import org.bluedb.api.keys.TimeKey;

/**
 * Function to mutate values in a time collection. Use this if you have to mutate a value in a
 * way that will result in a new time key. For example, if a value is no longer considered active or
 * the end time of the value is being changed. The new key must have the same time and
 * id or else the query will error out.
 * @param <V> the class of objects stored in collection as values
 */
@FunctionalInterface
public interface TimeEntityUpdater<V extends Serializable> {
	/**
	 * Function to mutate values in a time collection. Use this if you have to mutate a value in a
	 * way that will result in a new time key. For example, if a value is no longer considered active or
	 * the end time of the value is being changed. The new key must have the same time and
	 * id or else the query will error out.
	 * @param value collection value to be mutated
	 * @return the new key for the value. It must have the same time and id as before or else the 
	 * query will error out.
	 */
	public TimeKey update(V value);
}
