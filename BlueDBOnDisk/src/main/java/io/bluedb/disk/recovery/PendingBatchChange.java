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
			IndividualChange<T> firstChange = remainingChangesInOrder.peek(); //
			Segment<T> nextSegment = collection.getSegmentManager().getFirstSegment(firstChange.getKey());
			Range segmentRange = nextSegment.getRange();
			LinkedList<IndividualChange<T>> changesForSegment = getChangesBeforeOrAt(remainingChangesInOrder, segmentRange.getEnd());
			nextSegment.applyChanges(changesForSegment);
			removeChangesEndingBeforeOrAt(remainingChangesInOrder, segmentRange.getEnd());
		}
	}

	protected static <T extends Serializable> void removeChangesEndingBeforeOrAt(List<IndividualChange<T>> sortedChanges, long maxEndPoint) {
		Iterator<IndividualChange<T>> iterator = sortedChanges.iterator();
		while (iterator.hasNext()) {
			BlueKey nextChangeKey = iterator.next().getKey();
			boolean pastTheEndOfTheRange = nextChangeKey.getGroupingNumber() > maxEndPoint;
			if (pastTheEndOfTheRange) {
				break;
			}
			boolean stretchesPastEndPoint = nextChangeKey.isInRange(maxEndPoint + 1, Long.MAX_VALUE);
			if (!stretchesPastEndPoint) {
				iterator.remove();
			}
		}
	}

	protected static <T extends Serializable> LinkedList<IndividualChange<T>> getChangesBeforeOrAt( List<IndividualChange<T>> sortedChanges, long rangeEnd) {
		LinkedList<IndividualChange<T>> results = new LinkedList<>();
		Iterator<IndividualChange<T>> iterator = sortedChanges.iterator();
		while (iterator.hasNext()) {
			IndividualChange<T> nextChange = iterator.next();
			long groupingNumber = nextChange.getKey().getGroupingNumber();
			boolean pastTheEndOfTheRange = groupingNumber > rangeEnd;
			if (pastTheEndOfTheRange) {
				break;
			}
			results.add(nextChange);
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
