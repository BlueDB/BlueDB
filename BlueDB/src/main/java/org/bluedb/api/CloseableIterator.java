package org.bluedb.api;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Closeable Iterator allowing for safer iteration through a collection.
 * @param <V> the class of objects stored in collection as values
 */
public interface CloseableIterator<V> extends Closeable, Iterator<V> {
	public V peek();
}
