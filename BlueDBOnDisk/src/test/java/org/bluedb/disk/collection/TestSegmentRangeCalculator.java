package org.bluedb.disk.collection;

import org.bluedb.disk.segment.Range;

public class TestSegmentRangeCalculator {
	private long segmentRange;

	public TestSegmentRangeCalculator(long segmentRange) {
		this.segmentRange = segmentRange;
	}

	public Range calculateRange(long groupingNumber) {
		long rangeStart = groupingNumber - (groupingNumber % segmentRange);
		long rangeEnd = rangeStart + segmentRange - 1;
		return new Range(rangeStart, rangeEnd);
	}
}
