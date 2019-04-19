
package io.bluedb.disk.segment;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.bluedb.disk.recovery.IndividualChange;

public class SegmentBatch<T extends Serializable> {
	LinkedList<IndividualChange<T>> changeQueue;
	
	SegmentBatch(Collection<IndividualChange<T>> changes) {
		changeQueue = new LinkedList<IndividualChange<T>>(changes);
	}

	public List<ChunkBatch<T>> breakIntoChunks(List<Range> existingChunkRanges, List<Long> rollupLevels) {
		Set<Range> existingChunkRangesSet = new HashSet<>(existingChunkRanges);
		LinkedList<IndividualChange<T>> sortedQueue = new LinkedList<>(changeQueue);
		List<ChunkBatch<T>> results = new ArrayList<>();
		while (!sortedQueue.isEmpty()) {
			Range nextChunkRange = getNextRangeToUse(sortedQueue, existingChunkRangesSet, rollupLevels);
			LinkedList<IndividualChange<T>> changesInExistingRange = pollChangesBeforeOrAt(sortedQueue, nextChunkRange.getEnd());
			ChunkBatch<T> existingChunkUpdate = new ChunkBatch<T>(changesInExistingRange, nextChunkRange);
			results.add(existingChunkUpdate);
		}
		return results;
	}

	protected static <T extends Serializable> Range getNextRangeToUse(LinkedList<IndividualChange<T>> changeQueue, Set<Range> existingChunkRanges, List<Long> rollupLevels) {
		changeQueue = new LinkedList<>(changeQueue);  // to avoid mutation later
		long firstChangeGroupingNumber = changeQueue.peekFirst().getKey().getGroupingNumber();
		List<Range> possibleNextRanges = calculatePossibleChunkRanges(firstChangeGroupingNumber, rollupLevels);
		Range existingRange = findMatchingRange(possibleNextRanges, existingChunkRanges);
		if (existingRange != null) {
			return existingRange;
		}
		Range largestEmptyRange = getLargestEmptyRange(possibleNextRanges, existingChunkRanges);
		LinkedList<IndividualChange<T>> changesForRange = pollChangesBeforeOrAt(changeQueue, largestEmptyRange.getEnd());
		Range smallestRangeContainingSameChanges = chooseSmallestRangeContainingChanges(possibleNextRanges, changesForRange);
		return smallestRangeContainingSameChanges;
	}

	protected static List<Range> calculatePossibleChunkRanges(long groupingNumber, List<Long> rollupLevels) {
		return rollupLevels.stream()
				.sorted()
				.map( (rangeSize) -> Range.forValueAndRangeSize(groupingNumber, rangeSize))
				.collect(Collectors.toList())
				;
	}

	protected static Range getLargestEmptyRange(List<Range> optionsSmallToBig, Set<Range> existingChunkRanges) {
		return optionsSmallToBig.stream()
				.sorted(Comparator.reverseOrder())
				.filter( (candidateRange) -> !candidateRange.overlapsAny(existingChunkRanges) )
				.findFirst()
				.orElse(null);
	}

	protected static <T extends Serializable> Range chooseSmallestRangeContainingChanges(List<Range> rangeOptionsSmallToBig, List<IndividualChange<T>> nonEmptyChangeList) {
		return rangeOptionsSmallToBig.stream()
				.filter( (candidateRange) -> rangeContainsAll(candidateRange, nonEmptyChangeList) )
				.findFirst()
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
