package org.bluedb.api;

import java.io.Serializable;

/**
 * Function used to filter objects in queries.
 * @param <T> the class of objects stored in collection as values
 */
@FunctionalInterface
public interface Condition<T extends Serializable> {
	/**
	 * Filter function for queries.
	 * @param object value in the collection
	 * @return true if the value meets the requirement
	 */
	public boolean test(T object);
}
