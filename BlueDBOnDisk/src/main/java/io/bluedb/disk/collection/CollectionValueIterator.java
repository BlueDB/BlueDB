package io.bluedb.disk.collection;

import java.io.Serializable;
import java.util.List;
import io.bluedb.api.CloseableIterator;
import io.bluedb.api.Condition;
import io.bluedb.disk.lock.AutoCloseCountdown;
import io.bluedb.disk.segment.Range;

public class CollectionValueIterator<T extends Serializable> implements CloseableIterator<T> {

	private final static long TIMEOUT_DEFAULT_MILLIS = 15_000;
	private CollectionEntityIterator<T> entityIterator;
	private AutoCloseCountdown timeoutCloser;

	public CollectionValueIterator(BlueCollectionOnDisk<T> collection, Range range, boolean byStartTime, List<Condition<T>> objectConditions) {
		entityIterator = new CollectionEntityIterator<T>(collection, range, byStartTime, objectConditions);
		timeoutCloser = new AutoCloseCountdown(this, TIMEOUT_DEFAULT_MILLIS);
	}

	public CollectionValueIterator(BlueCollectionOnDisk<T> collection, Range range, long timeout, boolean byStartTime, List<Condition<T>> objectConditions) {
		entityIterator = new CollectionEntityIterator<T>(collection, range, byStartTime, objectConditions);
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
