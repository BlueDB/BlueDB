package org.bluedb.disk.recovery;

import java.io.Serializable;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.rollup.RollupTarget;

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
	public void apply(ReadWriteCollectionOnDisk<T> collection) throws BlueDbException {
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


	@Override
	public String toString() {
		return "<PendingIndexRollup for '" + indexName + "' index segment @ " + segmentGroupingNumber + ", " + min + " - " + max + ">";
	}
}
