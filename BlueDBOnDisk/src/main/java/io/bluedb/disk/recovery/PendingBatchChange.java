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
			IndividualChange<T> firstChange = remainingChangesInOrder.peek();
			Segment<T> nextSegment = collection.getSegmentManager().getFirstSegment(firstChange.getKey());
			Range segmentRange = nextSegment.getRange();
			LinkedList<IndividualChange<T>> changesForSegment = getChangesOverlappingRange(remainingChangesInOrder, segmentRange);
			nextSegment.applyChanges(changesForSegment);
			pollChangesInRange(remainingChangesInOrder, segmentRange);
		}
	}

	protected static <T extends Serializable> void removeChangesBeforeRange(List<IndividualChange<T>> sortedChanges, Range range) {
		Iterator<IndividualChange<T>> iterator = sortedChanges.iterator();
		while (iterator.hasNext()) {
			BlueKey nextChangeKey = iterator.next().getKey();
			boolean pastTheEndOfTheRange = nextChangeKey.getGroupingNumber() > range.getEnd();
			if (pastTheEndOfTheRange) {
				break;
			}
			boolean overlapsRange = nextChangeKey.isInRange(range.getStart(), range.getEnd());
			if (!overlapsRange) {
				iterator.remove();
			}
		}
	}

	public static <T extends Serializable> LinkedList<IndividualChange<T>> pollChangesInRange(LinkedList<IndividualChange<T>> inputs, Range range) {
		LinkedList<IndividualChange<T>> itemsInRange = new LinkedList<>();
		while (!inputs.isEmpty() && inputs.peek().getKey().isInRange(range.getStart(), range.getEnd())) {
			itemsInRange.add(inputs.poll());
		}
		return itemsInRange;
	}

	protected static <T extends Serializable> LinkedList<IndividualChange<T>> getChangesOverlappingRange( List<IndividualChange<T>> sortedChanges, Range range) {
		LinkedList<IndividualChange<T>> results = new LinkedList<>();
		Iterator<IndividualChange<T>> iterator = sortedChanges.iterator();
		while (iterator.hasNext()) {
			IndividualChange<T> nextChange = iterator.next();
			long groupingNumber = nextChange.getKey().getGroupingNumber();
			boolean pastTheEndOfTheRange = groupingNumber > range.getEnd();
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
