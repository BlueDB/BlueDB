package org.bluedb.disk.collection;

import java.io.Serializable;
import java.util.List;
import org.bluedb.api.CloseableIterator;
import org.bluedb.api.Condition;
import org.bluedb.disk.lock.AutoCloseCountdown;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadableSegmentManager;
import org.bluedb.disk.serialization.BlueEntity;

public class CollectionValueIterator<T extends Serializable> implements CloseableIterator<T> {

	private final static long TIMEOUT_DEFAULT_MILLIS = 15_000;
	private CollectionEntityIterator<T> entityIterator;
	private AutoCloseCountdown timeoutCloser;

	public CollectionValueIterator(ReadableSegmentManager<T> segmentManager, Range range, boolean byStartTime, List<Condition<T>> objectConditions) {
		entityIterator = new CollectionEntityIterator<T>(segmentManager, range, byStartTime, objectConditions);
		timeoutCloser = new AutoCloseCountdown(this, TIMEOUT_DEFAULT_MILLIS);
	}

	public CollectionValueIterator(ReadableSegmentManager<T> segmentManager, Range range, long timeout, boolean byStartTime, List<Condition<T>> objectConditions) {
		entityIterator = new CollectionEntityIterator<T>(segmentManager, range, byStartTime, objectConditions);
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
		if (entityIterator == null) {
			throw new RuntimeException("CollectionValueIterator has already been closed");
		}
		timeoutCloser.snooze();
		return entityIterator.hasNext();
	}

	@Override
	public T peek() {
		if (entityIterator == null) {
			throw new RuntimeException("CollectionValueIterator has already been closed");
		}
		timeoutCloser.snooze();
		BlueEntity<T> peekedEntity = entityIterator.peek();
		if (peekedEntity == null) {
			return null;
		} else {
			return peekedEntity.getValue();
		}
	}

	@Override
	public T next() {
		if (entityIterator == null) {
			throw new RuntimeException("CollectionValueIterator has already been closed");
		}
		timeoutCloser.snooze();
		return entityIterator.next().getValue();
	}
}
