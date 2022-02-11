package org.bluedb.api.datastructures;

import java.util.Iterator;

public class BlueSimpleIteratorWrapper<T> implements BlueSimpleIterator<T> {
	private Iterator<T> iterator;
	
	public BlueSimpleIteratorWrapper(Iterator<T> iterator) {
		this.iterator = iterator;
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public T next() {
		return iterator.next();
	}

	@Override
	public void close() { }

}
