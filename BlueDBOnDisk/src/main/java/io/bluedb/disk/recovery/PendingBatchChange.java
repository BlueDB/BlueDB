package io.bluedb.disk.recovery;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.segment.Range;
import io.bluedb.disk.segment.Segment;

public class PendingBatchChange<T extends Serializable> implements Serializable, Recoverable<T> {

	private static final long serialVersionUID = 1L;

	private List<IndividualChange<T>> sortedChanges;
	private long timeCreated;
	private long recoverableId;
	
	private PendingBatchChange(List<IndividualChange<T>> sortedChanges) {
		this.sortedChanges = sortedChanges;
		timeCreated = System.currentTimeMillis();
	}

	public static <T extends Serializable> PendingBatchChange<T> createBatchUpsert(List<IndividualChange<T>> sortedChanges){
		return new PendingBatchChange<T>(sortedChanges);
	}

	@Override
	public void apply(BlueCollectionOnDisk<T> collection) throws BlueDbException {
		LinkedList<IndividualChange<T>> remainingChangesInOrder = new LinkedList<>(sortedChanges);
		while (!remainingChangesInOrder.isEmpty()) {
			Segment<T> nextSegment = getNextSegment(collection, remainingChangesInOrder);
			LinkedList<IndividualChange<T>> changesForSegment = getChangesOverlappingSegment(remainingChangesInOrder, nextSegment);
			nextSegment.applyChanges(changesForSegment);
			removeChangesThatEndInOrBeforeSegment(remainingChangesInOrder, nextSegment);
		}
	}

	protected static <T extends Serializable> Segment<T> getNextSegment(BlueCollectionOnDisk<T> collection, LinkedList<IndividualChange<T>> sortedChanges) {
		BlueKey firstChangeKey = sortedChanges.peek().getKey();
		Segment<T> nextSegment = collection.getSegmentManager().getFirstSegment(firstChangeKey);
		return nextSegment;
	}

	protected static <T extends Serializable> void removeChangesThatEndInOrBeforeSegment(List<IndividualChange<T>> sortedChanges, Segment<T> segment) {
		Range segmentRange = segment.getRange();
		Range beyondSegment = new Range(segmentRange.getEnd() + 1, Long.MAX_VALUE);
		Iterator<IndividualChange<T>> iterator = sortedChanges.iterator();
		while (iterator.hasNext()) {
			IndividualChange<T> nextChange = iterator.next();
			if (!nextChange.overlaps(beyondSegment)) {
				iterator.remove();
			} else if (!nextChange.overlaps(segmentRange)) {
				return;  // we've past all the changes in this segment
			}
		}
	}

	protected static <T extends Serializable> LinkedList<IndividualChange<T>> getChangesOverlappingSegment( List<IndividualChange<T>> sortedChanges, Segment<T> segment) {
		long segmentEnd = segment.getRange().getEnd();
		LinkedList<IndividualChange<T>> results = new LinkedList<>();
		for (IndividualChange<T> change: sortedChanges) {
			if (change.getGroupingNumber() > segmentEnd) {
				break;
			}
			results.add(change);
		}
		return results;
	}

	@Override
	public long getTimeCreated() {
		return timeCreated;
	}

	@Override
	public String toString() {
		return "<" + this.getClass().getSimpleName() + " for " + sortedChanges.size() + " values>";
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
