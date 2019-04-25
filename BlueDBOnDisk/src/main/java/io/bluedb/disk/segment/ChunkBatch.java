package io.bluedb.disk.segment;

import java.io.Serializable;
import java.util.List;

import io.bluedb.disk.recovery.IndividualChange;

public class ChunkBatch<T extends Serializable> {

	private final List<IndividualChange<T>> changesInOrder;
	private final Range range;

	public ChunkBatch(List<IndividualChange<T>> changesInOrder, Range range) {
		this.changesInOrder = changesInOrder;
		this.range = range;
	}

	public List<IndividualChange<T>> getChangesInOrder() {
		return changesInOrder;
	}

	public Range getRange() {
		return range;
	}
}
