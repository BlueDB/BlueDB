package org.bluedb.api;

import java.io.Closeable;
import java.util.Iterator;

import org.bluedb.api.exceptions.BlueDbException;

/**
 * Closeable Iterator allowing for safer iteration through a collection.
 * @param <V> the class of objects stored in collection as values
 */
public interface CloseableIterator<V> extends Closeable, Iterator<V> {
	public V peek();
	
	public void keepAlive();

	public default int countRemainderAndClose() throws BlueDbException {
		try (CloseableIterator<V> iterToClose = this) {
			int count = 0;
			while (hasNext()) {
				count++;
				next();
			}
			return count;
		} catch (Throwable t) {
			throw new BlueDbException(t.getMessage(), t);
		}
	}
	
	@Override void close();
}
