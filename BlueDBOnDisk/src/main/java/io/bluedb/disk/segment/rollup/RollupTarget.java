package io.bluedb.disk.segment.rollup;

import io.bluedb.disk.segment.Range;

public class RollupTarget {

	private final long segmentGroupingNumber;
	private final Range range;

	public RollupTarget(long segmentGroupingNumber, Range range) {
		this.segmentGroupingNumber = segmentGroupingNumber;
		this.range = range;
	}

	public long getSegmentGroupingNumber() {
		return segmentGroupingNumber;
	}

	public Range getRange() {
		return range;
	}

	public long getWriteRollupDelay() {
		return range.length();
	}

	public long getReadRollupDelay() {
		return range.length();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((range == null) ? 0 : range.hashCode());
		result = prime * result + (int) (segmentGroupingNumber ^ (segmentGroupingNumber >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj != null && getClass().equals(obj.getClass())) {
			RollupTarget other = (RollupTarget) obj;
			return (segmentGroupingNumber == other.getSegmentGroupingNumber()) && (range.equals(other.getRange())); 
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "[" + this.getClass().getSimpleName() + " @ " + segmentGroupingNumber + " " + range.toString() + "]";
	}
}
