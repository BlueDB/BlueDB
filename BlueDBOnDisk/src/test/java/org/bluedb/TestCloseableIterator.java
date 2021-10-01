package org.bluedb;

import java.util.Iterator;

import org.bluedb.api.CloseableIterator;

public class TestCloseableIterator<T> implements CloseableIterator<T> {
	
	private final Iterator<T> iterator;
	private T next;
	
	public TestCloseableIterator(Iterator<T> iterator) {
		this.iterator = iterator;
	}

	@Override
	public boolean hasNext() {
		return next != null || iterator.hasNext();
	}

	@Override
	public T peek() {
		if(next == null) {
			next = iterator.next();
		}
		return next;
	}

	@Override
	public T next() {
		if(next != null) {
			T result = next;
			next = null;
			return result;
		} else {
			return iterator.next();
		}
	}

	@Override
	public void keepAlive() { }

	@Override
	public void close() { }

}
