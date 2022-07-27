package org.bluedb.api;

import java.io.Serializable;

import org.bluedb.api.datastructures.TimeKeyValuePair;

/**
 * Function to replace values in a time collection. Use this if you have to replace a value in a
 * way that will result in a new time key. For example, if a value is no longer considered active or
 * the end time of the value is being changed. The replacement key must have the same time and
 * id or else the query will error out.
 * @param <V> the class of objects stored in collection as values
 */
@FunctionalInterface
public interface TimeEntityMapper<V extends Serializable> {
	/**
	 * Function to replace the values in a time collection. Use this if you have to replace a value in a
	 * way that will result in a new time key. For example, if a value is no longer considered active or
	 * the end time of the value is being changed. The replacement key must have the same time and
	 * id or else the query will error out.
	 * @param value collection value to be replaced
	 * @return replacement key and value. The replacement key must have the same time and
	 * id or else the query will error out.
	 */
	public TimeKeyValuePair<V> map(V value);
}
