package org.bluedb.disk.segment.rollup;

import java.util.Objects;
import org.bluedb.disk.segment.Range;

public class IndexRollupTarget extends RollupTarget {

	private final String indexName;

	public IndexRollupTarget(String indexName, long segmentGroupingNumber, Range range) {
		super(segmentGroupingNumber, range);
		this.indexName = indexName;
	}

	public String getIndexName() {
		return indexName;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((getRange() == null) ? 0 : getRange().hashCode());
		result = prime * result + (int) (getSegmentGroupingNumber() ^ (getSegmentGroupingNumber() >>> 32));
		result = prime * result + ((indexName == null) ? 0 : indexName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof IndexRollupTarget) {
			IndexRollupTarget other = (IndexRollupTarget) obj;
			return getSegmentGroupingNumber() == other.getSegmentGroupingNumber()
					&& Objects.equals(getRange(), other.getRange())
					&& Objects.equals(indexName, other.indexName);
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "[" + this.getClass().getSimpleName() + " in " + indexName + " @ " + getSegmentGroupingNumber() + " " + getRange().toString() + "]";
	}
}
