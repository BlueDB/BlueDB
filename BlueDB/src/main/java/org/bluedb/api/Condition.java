package org.bluedb.api;

import java.io.Serializable;

/**
 * Function used to filter objects in queries.
 * @param <V> the class of objects stored in collection as values
 */
@FunctionalInterface
public interface Condition<V extends Serializable> {
	/**
	 * Filter function for queries.
	 * @param value - collection value to be tested
	 * @return true if the value meets the requirement
	 */
	public boolean test(V value);
}
