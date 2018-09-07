package io.bluedb.disk.recovery;

import java.io.Serializable;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.segment.Range;
import io.bluedb.disk.segment.rollup.RollupTarget;

public class PendingIndexRollup<T extends Serializable> implements Serializable, Recoverable<T>{

	private static final long serialVersionUID = 1L;

	final long timeCreated;
	final long segmentGroupingNumber;
	final long min;
	final long max;
	long recoverableId;
	final String indexName;

	public PendingIndexRollup(String indexName, long segmentGroupingNumber, long min, long max) {
		timeCreated = System.currentTimeMillis();
		this.segmentGroupingNumber = segmentGroupingNumber;
		this.min = min;
		this.max = max;
		this.indexName = indexName;
	}

	public PendingIndexRollup(String indexName, RollupTarget rollupTarget) {
		this(indexName, rollupTarget.getSegmentGroupingNumber(), rollupTarget.getRange().getStart(), rollupTarget.getRange().getEnd());
	}

	@Override
	public void apply(BlueCollectionOnDisk<T> collection) throws BlueDbException {
		Range range = new Range(min, max);
		collection.rollupIndex(indexName, range);
	}

	@Override
	public long getTimeCreated() {
		return timeCreated;
	}

	@Override
	public long getRecoverableId() {
		return recoverableId;
	}

	@Override
	public void setRecoverableId(long recoverableId) {
		this.recoverableId = recoverableId;
	}
}
