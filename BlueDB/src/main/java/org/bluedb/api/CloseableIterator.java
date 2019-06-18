package org.bluedb.api;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Closeable Iterator allowing for safer iteration through a collection.
 * @param <E> the class of objects stored in collection as values
 */
public interface CloseableIterator<E> extends Closeable, Iterator<E> {
}
