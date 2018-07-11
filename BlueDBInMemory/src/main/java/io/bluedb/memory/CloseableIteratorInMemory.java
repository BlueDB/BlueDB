package io.bluedb.memory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import io.bluedb.api.CloseableIterator;

public class CloseableIteratorInMemory<E extends Serializable> implements CloseableIterator<E> {

	Iterator<E> listIterator;

	CloseableIteratorInMemory(List<E> list) {
		List<E> copy = new ArrayList<E>(list);
		listIterator = copy.iterator();
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public boolean hasNext() {
		return listIterator.hasNext();
	}

	@Override
	public E next() {
		return listIterator.next();
	}

}
