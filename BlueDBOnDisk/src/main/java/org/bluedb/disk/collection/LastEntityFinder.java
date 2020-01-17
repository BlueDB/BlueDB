package org.bluedb.disk.collection;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import org.bluedb.disk.collection.index.ReadableBlueIndexOnDisk;
import org.bluedb.disk.segment.ReadableSegment;
import org.bluedb.disk.segment.ReadableSegmentManager;
import org.bluedb.disk.segment.SegmentEntityIterator;
import org.bluedb.disk.serialization.BlueEntity;

public class LastEntityFinder {

	final private ReadableSegmentManager<?> segmentManager;

	public LastEntityFinder(final ReadableBlueIndexOnDisk<?, ?> index) {
		segmentManager = index.getSegmentManager();
	}

	public LastEntityFinder(final ReadableBlueCollectionOnDisk<?> collection) {
		segmentManager = collection.getSegmentManager();
	}

	public BlueEntity<?> getLastEntity() {
		List<ReadableSegment<Serializable>> segments = getSegmentsInReverseOrder();
		while (!segments.isEmpty()) {
			BlueEntity<?> last = null;
			ReadableSegment<?> segment = segments.remove(0);
			try (SegmentEntityIterator<?> segmentIterator = segment.getIterator(Long.MIN_VALUE, Long.MAX_VALUE)) {
				while(segmentIterator.hasNext()) {
					last = segmentIterator.next();
				}
			}
			if (last != null) {
				return last;
			}
		}
		return null;
	}

	public List<ReadableSegment<Serializable>> getSegmentsInReverseOrder() {
		List<?> existingSegmentsUntyped = segmentManager.getAllExistingSegments();
		@SuppressWarnings("unchecked")
		List<ReadableSegment<Serializable>> segments = (List<ReadableSegment<Serializable>>) existingSegmentsUntyped;
		Collections.sort(segments);
		Collections.reverse(segments);
		return segments;
	}
}
