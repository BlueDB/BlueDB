package io.bluedb.disk.segment.rollup;

import io.bluedb.disk.segment.Range;

public interface Rollupable {
	public void reportRead(long segmentGroupingNumber, Range range);
	public void reportWrite(long segmentGroupingNumber, Range range);
}
