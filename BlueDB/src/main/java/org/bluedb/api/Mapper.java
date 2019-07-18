package org.bluedb.api;

import java.io.Serializable;

/**
 * Function to replace value objects in a collection
 * @param <V> the class of objects stored in collection as values
 */
@FunctionalInterface
public interface Mapper<V extends Serializable> {
	/**
	 * Function to replace a value in a collection.
	 * @param value - collection value to be replaced
	 * @return replacement value
	 */
	public V update(V value);
}
