package org.bluedb.api.datastructures;

import java.io.Closeable;
import java.util.Iterator;

/**
 * This simple iterator abstraction provides a way to iterate over the objects and close and
 * resources when finished. It is used by the {@link BlueSimpleSet} class.
 * @param <T> The type of data it iterates over.
 */
public interface BlueSimpleIterator<T> extends Iterator<T>, Closeable {
	@Override
	void close(); //Close shouldn't throw anything
}
