package io.bluedb.disk.collection;

import java.io.Serializable;

import io.bluedb.api.CloseableIterator;
import io.bluedb.disk.lock.AutoCloseCountdown;
import io.bluedb.disk.segment.Range;

public class CollectionValueIterator<T extends Serializable> implements CloseableIterator<T> {

	private final static long TIMEOUT_DEFAULT_MILLIS = 15_000;
	private CollectionEntityIterator<T> entityIterator;
	private AutoCloseCountdown timeoutCloser;
	
	public CollectionValueIterator(BlueCollectionOnDisk<T> collection, Range range) {
		entityIterator = new CollectionEntityIterator<T>(collection, range);
		timeoutCloser = new AutoCloseCountdown(this, TIMEOUT_DEFAULT_MILLIS);
	}

	public CollectionValueIterator(BlueCollectionOnDisk<T> collection, Range range, long timeout) {
		entityIterator = new CollectionEntityIterator<T>(collection, range);
		timeoutCloser = new AutoCloseCountdown(this, timeout);
	}

	@Override
	public void close() {
		if (entityIterator != null) {
			entityIterator.close();
			entityIterator = null;
		}
		timeoutCloser.cancel();
	}

	@Override
	public boolean hasNext() {
		timeoutCloser.snooze();
		return entityIterator.hasNext();
	}

	@Override
	public T next() {
		timeoutCloser.snooze();
		return entityIterator.next().getValue();
	}
}
