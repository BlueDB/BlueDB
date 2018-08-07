package io.bluedb.disk.collection;

import java.io.Closeable;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import io.bluedb.disk.segment.SegmentEntityIterator;
import io.bluedb.disk.segment.Segment;
import io.bluedb.disk.serialization.BlueEntity;

public class CollectionEntityIterator<T extends Serializable> implements Iterator<BlueEntity<T>>, Closeable {

	final private List<Segment<T>> segments;
	final private long min;
	final private long max;
	private long endGroupingValueOfCompletedSegments;
	private SegmentEntityIterator<T> segmentIterator;
	private BlueEntity<T> next;

	public CollectionEntityIterator(final BlueCollectionOnDisk<T> collection, final long min, final long max) {
		this.min = min;
		this.max = max;
		this.endGroupingValueOfCompletedSegments = Long.MIN_VALUE;
		segments = collection.getSegmentManager().getExistingSegments(min, max);
		Collections.sort(segments);
	}

	@Override
	public void close() {
		if (segmentIterator != null) {
			segmentIterator.close();
		}
	}

	@Override
	public boolean hasNext() {
		if (next == null) {
			next = nextFromSegment();
		}
		return next != null;
	}

	@Override
	public BlueEntity<T> next() {
		if (next == null) {
			next = nextFromSegment();
		}
		BlueEntity<T> response = next;
		next = null;
		return response;
	}

	private BlueEntity<T> nextFromSegment() {
		while (!segments.isEmpty() || segmentIterator != null) {
			if (segmentIterator != null && segmentIterator.hasNext()) {
				BlueEntity<T> result = segmentIterator.next();
				return result;
			}
			if (segmentIterator != null) {
				segmentIterator.close();
			}
			segmentIterator = getNextSegmentIterator();
		}
		return null;
	}

	private SegmentEntityIterator<T> getNextSegmentIterator() {
		if (segments.isEmpty()) {
			return null;
		}
		if (segmentIterator != null) {
			long endOfLastSegment =  segmentIterator.getSegment().getRange().getEnd();
			endGroupingValueOfCompletedSegments = endOfLastSegment;
		}
		Segment<T> segment = segments.remove(0);
		return segment.getIterator(endGroupingValueOfCompletedSegments, min, max);
	}
}
