
package io.bluedb.disk.segment;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.recovery.IndividualChange;

public class SegmentBatch<T extends Serializable> {
	LinkedList<IndividualChange<T>> changeQueue;
	
	SegmentBatch(Collection<IndividualChange<T>> changes) {
		changeQueue = new LinkedList<IndividualChange<T>>(changes);
	}

	public List<ChunkBatch<T>> breakIntoChunks(List<Range> existingChunkRanges, List<Long> rollupLevels) {
		LinkedList<IndividualChange<T>> sortedQueue = new LinkedList<>(changeQueue);
		List<ChunkBatch<T>> results = new ArrayList<>();
		while (!sortedQueue.isEmpty()) {
			Range nextChunkRange = getNextRangeToUse(sortedQueue, existingChunkRanges, rollupLevels);
			LinkedList<IndividualChange<T>> changesInExistingRange = pollChangesInRange(sortedQueue, nextChunkRange);
			ChunkBatch<T> existingChunkUpdate = new ChunkBatch<T>(changesInExistingRange, nextChunkRange);
			results.add(existingChunkUpdate);
		}
		return results;
	}

	protected static <T extends Serializable> Range getNextRangeToUse(LinkedList<IndividualChange<T>> changeQueue, List<Range> existingChunkRanges, List<Long> rollupLevels) {
		changeQueue = new LinkedList<>(changeQueue);
		BlueKey firstKey = changeQueue.peekFirst().getKey();
		long firstChangeGroupingNumber = firstKey.getGroupingNumber();
		Range existingRange = findMatchingRange(firstChangeGroupingNumber, existingChunkRanges);
		if (existingRange != null) {
			return existingRange;
		}
		Range largestEmptyRange = getLargestEmptyRangeContaining(firstChangeGroupingNumber, existingChunkRanges, rollupLevels);
		LinkedList<IndividualChange<T>> itemsForChunk = pollChangesInRange(changeQueue, largestEmptyRange);
		Range smallestRangeContainingSameChanges = getSmallestRangeContaining(itemsForChunk, rollupLevels);
		return smallestRangeContainingSameChanges;
	}

	protected static Range getLargestEmptyRangeContaining(long groupingNumber, List<Range> existingChunkRanges, List<Long> rollupLevels) {
		Range largestKnownEmptyRange = null;
		for (long rollupLevel: rollupLevels) {
			Range nextLargerRange = Range.forValueAndRangeSize(groupingNumber, rollupLevel);
			if (nextLargerRange.overlapsAny(existingChunkRanges)) {
				return largestKnownEmptyRange;
			}
			largestKnownEmptyRange = nextLargerRange;
		}
		return largestKnownEmptyRange;
	}

	protected static <T extends Serializable> Range getSmallestRangeContaining(List<IndividualChange<T>> nonEmptyChangeList, List<Long> rollupLevels) {
		long firstGroupingNumber = nonEmptyChangeList.get(0).getKey().getGroupingNumber();
		Range smallestAcceptableRange = null;
		for (Long rollupLevel: Blutils.reversed(rollupLevels)) {
			Range nextRangeDown = Range.forValueAndRangeSize(firstGroupingNumber, rollupLevel);
			if (!rangeContainsAll(nextRangeDown, nonEmptyChangeList)) {
				return smallestAcceptableRange;
			}
			smallestAcceptableRange = nextRangeDown;
		}
		return smallestAcceptableRange;
	}

	protected static <T extends Serializable> boolean rangeContainsAll(Range range, List<IndividualChange<T>> changes) {
		for (IndividualChange<?> change: changes) {
			long changeGroupingNumber = change.getKey().getGroupingNumber();
			if (!range.containsInclusive(changeGroupingNumber)) {
				return false;
			}
		}
		return true;
	}

	protected static Range findMatchingRange(long groupingNumber, List<Range> ranges) {
		for (Range range: ranges) {
			if (range.containsInclusive(groupingNumber)) {
				return range;
			}
		}
		return null;
	}


	public static <T extends Serializable> LinkedList<IndividualChange<T>> pollChangesInRange(LinkedList<IndividualChange<T>> inputs, Range range) {
		LinkedList<IndividualChange<T>> itemsInRange = new LinkedList<>();
		while (!inputs.isEmpty() && inputs.peek().getKey().isInRange(range.getStart(), range.getEnd())) {
			itemsInRange.add(inputs.poll());
		}
		return itemsInRange;
	}
}
