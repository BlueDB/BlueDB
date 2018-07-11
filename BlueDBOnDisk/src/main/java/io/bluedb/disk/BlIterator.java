package io.bluedb.disk;

import java.io.IOException;
import java.io.Serializable;
import io.bluedb.api.CloseableIterator;
import io.bluedb.disk.collection.BlueCollectionImpl;
import io.bluedb.disk.collection.CollectionEntityIterator;

public class BlIterator<T extends Serializable> implements CloseableIterator<T> {

	private final CollectionEntityIterator<T> entityIterator;
	
	public BlIterator(BlueCollectionImpl<T> collection, long min, long max) {
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
