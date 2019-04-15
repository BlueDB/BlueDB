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
import io.bluedb.disk.segment.SegmentBatch;

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
		LinkedList<IndividualChange<T>> unqueuedChanges = new LinkedList<>(sortedChanges);
		while (!unqueuedChanges.isEmpty()) {
			Segment<T> nextSegment = getFirstSegmentAffected(collection, unqueuedChanges);
			LinkedList<IndividualChange<T>> queuedChanges = pollChangesInSegment(unqueuedChanges, nextSegment);
			while (!queuedChanges.isEmpty()) {
				nextSegment.applyChanges(queuedChanges);
				removeChangesThatEndInOrBeforeSegment(queuedChanges, nextSegment);
				nextSegment = collection.getSegmentManager().getSegmentAfter(nextSegment);
				queuedChanges.addAll( pollChangesInSegment(unqueuedChanges, nextSegment) );
			}
		}
	}

	public static <T extends Serializable> LinkedList<IndividualChange<T>> pollChangesInSegment(LinkedList<IndividualChange<T>> sortedChanges, Segment<T> segment) {
		long maxGroupingNumber = segment.getRange().getEnd();
		return SegmentBatch.pollChangesBeforeOrAt(sortedChanges, maxGroupingNumber);
	}

	protected static <T extends Serializable> Segment<T> getFirstSegmentAffected(BlueCollectionOnDisk<T> collection, LinkedList<IndividualChange<T>> sortedChanges) {
		BlueKey firstChangeKey = sortedChanges.peek().getKey();
		Segment<T> firstSegment = collection.getSegmentManager().getFirstSegment(firstChangeKey);
		return firstSegment;
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
