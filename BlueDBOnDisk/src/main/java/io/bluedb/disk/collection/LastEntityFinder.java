package io.bluedb.disk.collection;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import io.bluedb.disk.segment.SegmentEntityIterator;
import io.bluedb.disk.segment.Segment;
import io.bluedb.disk.serialization.BlueEntity;

public class LastEntityFinder<T extends Serializable> {

	final private List<Segment<T>> segments;

	public LastEntityFinder(final BlueCollectionOnDisk<T> collection) {
		segments = collection.getSegmentManager().getExistingSegments(Long.MIN_VALUE, Long.MAX_VALUE);
		Collections.sort(segments);
		Collections.reverse(segments);
	}

	public BlueEntity<T> getLastEntity() {
		while (!segments.isEmpty()) {
			BlueEntity<T> last = null;
			Segment<T> segment = segments.remove(0);
			try (SegmentEntityIterator<T> segmentIterator = segment.getIterator(Long.MIN_VALUE, Long.MAX_VALUE)) {
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
}
