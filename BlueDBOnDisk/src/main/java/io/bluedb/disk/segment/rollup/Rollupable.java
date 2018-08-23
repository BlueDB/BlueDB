package io.bluedb.disk.segment.rollup;

public interface Rollupable {
	public void scheduleRollup(RollupTarget target);
}
