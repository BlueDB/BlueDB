package io.bluedb.disk.collection;

import java.io.IOException;
import java.io.Serializable;
import io.bluedb.api.CloseableIterator;

public class CollectionValueIterator<T extends Serializable> implements CloseableIterator<T> {

	private final CollectionEntityIterator<T> entityIterator;
	
	public CollectionValueIterator(BlueCollectionImpl<T> collection, long min, long max) {
		entityIterator = new CollectionEntityIterator<T>(collection, min, max);
	}

	@Override
	public void close() {
		entityIterator.close();
	}

	@Override
	public boolean hasNext() {
		return entityIterator.hasNext();
	}

	@Override
	public T next() {
		return entityIterator.next().getValue();
	}

}
