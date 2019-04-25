package org.bluedb.disk.segment.rollup;

import java.util.List;

public interface Rollupable {
	public void reportReads(List<RollupTarget> rollupTargets);
	public void reportWrites(List<RollupTarget> rollupTargets);
}
