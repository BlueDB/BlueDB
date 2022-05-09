package org.bluedb.disk.segment;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.segment.path.SegmentPathManager;
import org.bluedb.disk.segment.path.SegmentSizeConfiguration;

public abstract class ReadableSegmentManager<T extends Serializable> {

	protected final SegmentPathManager pathManager;

	public abstract ReadableSegment<T> getFirstSegment(BlueKey key);
	public abstract ReadableSegment<T> getSegment(long groupingNumber);
	public abstract List<? extends ReadableSegment<T>> getAllExistingSegments();
	public abstract List<? extends ReadableSegment<T>> getExistingSegments(Range range, Optional<Set<Range>> segmentRangesToInclude);
	public abstract List<Range> getExistingSegmentRanges(Range range, Optional<Set<Range>> segmentRangesToInclude);


	public ReadableSegmentManager(Path collectionPath, SegmentSizeConfiguration sizeConfig) {
		this.pathManager = createSegmentPathManager(sizeConfig, collectionPath);
	}

	public Range getSegmentRange(long groupingValue) {
		return Range.forValueAndRangeSize(groupingValue, getSegmentSize());
	}

	public SegmentPathManager getPathManager() {
		return pathManager;
	}

	public Range toRange(Path path) {
		String filename = path.toFile().getName();
		long rangeStart = Long.valueOf(filename) * getSegmentSize();
		Range range = new Range(rangeStart, rangeStart + getSegmentSize() - 1);
		return range;
	}

	public long getSegmentSize() {
		return pathManager.getSegmentSize();
	}

	protected static SegmentPathManager createSegmentPathManager(SegmentSizeConfiguration sizeConfig, Path collectionPath) {
		return new SegmentPathManager(collectionPath, sizeConfig);
	}
}
