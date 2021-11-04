package org.bluedb.api.datastructures;

import java.util.Iterator;

/**
 * This simple set abstraction provides a way to iterate over the objects inside of it and to
 * check if it contains an object. This exists in order to provide BlueDB users with an option
 * to provide a set of objects as part of a query without requiring all of those objects to be
 * in memory at one time. You would achieve that by creating your own BlueSimpleSet implementation.
 * @param <T> The type of data contained in the simple set.
 */
public interface BlueSimpleSet<T> {
	public Iterator<T> iterator();
	public boolean contains(T object);
}
