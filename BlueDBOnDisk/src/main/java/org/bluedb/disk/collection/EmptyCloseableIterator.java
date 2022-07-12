package org.bluedb.disk.collection;

import org.bluedb.api.CloseableIterator;

public class EmptyCloseableIterator<V> implements CloseableIterator<V> {

	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public V next() {
		return null;
	}

	@Override
	public V peek() {
		return null;
	}

	@Override
	public void keepAlive() {
		
	}

	@Override
	public void close() {
		
	}

}
