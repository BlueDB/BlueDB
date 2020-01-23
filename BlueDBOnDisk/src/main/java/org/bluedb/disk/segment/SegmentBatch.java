
package org.bluedb.disk.segment;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.bluedb.disk.recovery.IndividualChange;

public class SegmentBatch<T extends Serializable> {
	LinkedList<IndividualChange<T>> changeQueue;
	
	private final static Comparator<Range> orderBySize = (r1, r2) -> Long.compare(r1.length(), r2.length());

	SegmentBatch(Collection<IndividualChange<T>> changes) {
		changeQueue = new LinkedList<IndividualChange<T>>(changes);
	}

	public List<ChunkBatch<T>> breakIntoChunks(List<Range> existingChunkRanges, ReadableSegment<T> segment) {
		Set<Range> existingChunkRangesSet = new HashSet<>(existingChunkRanges);
		LinkedList<IndividualChange<T>> sortedQueue = new LinkedList<>(changeQueue);
		List<ChunkBatch<T>> results = new ArrayList<>();
		while (!sortedQueue.isEmpty()) {
			Range nextChunkRange = getNextRangeToUse(sortedQueue, existingChunkRangesSet, segment);
			LinkedList<IndividualChange<T>> changesInExistingRange = pollChangesBeforeOrAt(sortedQueue, nextChunkRange.getEnd());
			ChunkBatch<T> existingChunkUpdate = new ChunkBatch<T>(changesInExistingRange, nextChunkRange);
			results.add(existingChunkUpdate);
		}
		return results;
	}

	protected static <T extends Serializable> Range getNextRangeToUse(LinkedList<IndividualChange<T>> changeQueue, Set<Range> existingChunkRanges, ReadableSegment<T> segment) {
		changeQueue = new LinkedList<>(changeQueue);  // to avoid mutation later
		long firstChangeGroupingNumber = changeQueue.peekFirst().getGroupingNumber();
		List<Range> possibleNextRanges = segment.calculatePossibleChunkRanges(firstChangeGroupingNumber);
		Range existingRange = findMatchingRange(possibleNextRanges, existingChunkRanges);
		if (existingRange != null) {
			return existingRange;
		}
		Range largestEmptyRange = getLargestEmptyRange(possibleNextRanges, existingChunkRanges);
		LinkedList<IndividualChange<T>> changesForRange = pollChangesBeforeOrAt(changeQueue, largestEmptyRange.getEnd());
		Range smallestRangeContainingSameChanges = chooseSmallestRangeContainingChanges(possibleNextRanges, changesForRange);
		return smallestRangeContainingSameChanges;
	}

	protected static Range getLargestEmptyRange(List<Range> rangeOptions, Set<Range> existingChunkRanges) {
		return rangeOptions.stream()
				.filter( (candidateRange) -> !candidateRange.overlapsAny(existingChunkRanges) )
				.max(orderBySize)
				.orElse(null);
	}

	protected static <T extends Serializable> Range chooseSmallestRangeContainingChanges(List<Range> rangeOptions, List<IndividualChange<T>> nonEmptyChangeList) {
		return rangeOptions.stream()
				.filter( (candidateRange) -> rangeContainsAll(candidateRange, nonEmptyChangeList) )
				.min(orderBySize)
				.get();  // this will throw an exception if there is none, should never happen because we chose changes based on the range
	}

	protected static <T extends Serializable> boolean rangeContainsAll(Range range, List<IndividualChange<T>> changes) {
		return changes.stream()
				.map( (chg) -> chg.getGroupingNumber() )
				.allMatch( (groupingNumber) -> range.containsInclusive(groupingNumber) );
	}

	protected static Range findMatchingRange(List<Range> options, Set<Range> ranges) {
		return options.stream()
				.filter( (candidateRange) -> ranges.contains(candidateRange) )
				.findFirst()
				.orElse(null);
	}

	public static <T extends Serializable> LinkedList<IndividualChange<T>> pollChangesBeforeOrAt(LinkedList<IndividualChange<T>> sortedChanges, long maxGroupingNumber) {
		LinkedList<IndividualChange<T>> itemsInRange = new LinkedList<>();
		while (!sortedChanges.isEmpty() && sortedChanges.peek().getGroupingNumber() <= maxGroupingNumber) {
			itemsInRange.add(sortedChanges.poll());
		}
		return itemsInRange;
	}
}
