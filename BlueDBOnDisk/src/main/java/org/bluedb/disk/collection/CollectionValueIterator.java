package org.bluedb.disk.collection;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bluedb.api.CloseableIterator;
import org.bluedb.api.Condition;
import org.bluedb.disk.lock.AutoCloseCountdown;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadableSegmentManager;
import org.bluedb.disk.serialization.BlueEntity;

public class CollectionValueIterator<T extends Serializable> implements CloseableIterator<T> {
	
	private final static long TIMEOUT_DEFAULT_MILLIS = 15_000;
	private final CollectionEntityIterator<T> entityIterator;
	private final AutoCloseCountdown timeoutCloser;
	
	private AtomicBoolean hasClosed = new AtomicBoolean(false);
	
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
		if(!hasClosed.getAndSet(true)) {
			entityIterator.close();
			timeoutCloser.cancel();
		}
	}

	@Override
	public boolean hasNext() {
		if (hasClosed.get()) {
			throw new RuntimeException("CollectionValueIterator has already been closed");
		}
		
		try {
			timeoutCloser.setToWaitingOnBlueDb();
			return entityIterator.hasNext();
		} finally {
			timeoutCloser.setToWaitingOnClient();
		}
	}

	@Override
	public T peek() {
		if (hasClosed.get()) {
			throw new RuntimeException("CollectionValueIterator has already been closed");
		}
		
		try {
			timeoutCloser.setToWaitingOnBlueDb();
			BlueEntity<T> peekedEntity = entityIterator.peek();
			if (peekedEntity == null) {
				return null;
			} else {
				return peekedEntity.getValue();
			}
		} finally {
			timeoutCloser.setToWaitingOnClient();
		}
	}

	@Override
	public T next() {
		if (hasClosed.get()) {
			throw new RuntimeException("CollectionValueIterator has already been closed");
		}
		
		try {
			timeoutCloser.setToWaitingOnBlueDb();
			return entityIterator.next().getValue();
		} finally {
			timeoutCloser.setToWaitingOnClient();
		}
	}
	
	@Override
	public void keepAlive() {
		timeoutCloser.snooze();
	}
}
