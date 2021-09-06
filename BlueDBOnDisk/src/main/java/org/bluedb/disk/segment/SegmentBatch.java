
package org.bluedb.disk.segment;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.recovery.SortedChangeSupplier;

public class SegmentBatch<T extends Serializable> {
	
	private SortedChangeSupplier<T> sortedChanges;
	private PossibleChunkRangeCalculator possibleChunkRangeCalculator;
	private Set<Range> existingChunkRanges;
	
	SegmentBatch(SortedChangeSupplier<T> sortedChanges, PossibleChunkRangeCalculator possibleChunkRangeCalculator, ExistingChunkRangeFinder existingChunkRangeFinder) {
		this.sortedChanges = sortedChanges;
		this.possibleChunkRangeCalculator = possibleChunkRangeCalculator;
		this.existingChunkRanges = new HashSet<>(existingChunkRangeFinder.findExistingChunkRanges());
	}
	
	SegmentBatch(SortedChangeSupplier<T> sortedChanges, ReadableSegment<T> segment) {
		this.sortedChanges = sortedChanges;
		this.possibleChunkRangeCalculator = segment::calculatePossibleChunkRanges;
		this.existingChunkRanges = new HashSet<>(ReadableSegment.getAllFileRangesInOrder(segment.getPath()));
	}

	public Optional<Range> determineNextChunkRange() throws BlueDbException {
		Optional<IndividualChange<T>> nextChange = sortedChanges.getNextChange();
		if(nextChange.isPresent()) {
			long firstChangeGroupingNumber = nextChange.get().getGroupingNumber();
			List<Range> possibleNextRanges = possibleChunkRangeCalculator.calculatePossibleChunkRangesForGroupingNumber(firstChangeGroupingNumber);
			Optional<Range> existingRange = findMatchingRange(possibleNextRanges, existingChunkRanges);
			if (existingRange.isPresent()) {
				return existingRange;
			}
			
			Range largestEmptyRange = getLargestEmptyRange(possibleNextRanges, existingChunkRanges);
			Set<Long> changeGroupingNumbersForRange = sortedChanges.findGroupingNumbersForNextChangesBeforeOrAtGroupingNumber(largestEmptyRange.getEnd());
			return chooseSmallestRangeContainingChanges(possibleNextRanges, changeGroupingNumbersForRange);
		}
		
		return Optional.empty();
	}

	protected static Range getLargestEmptyRange(List<Range> rangeOptions, Set<Range> existingChunkRanges) {
		return rangeOptions.stream()
				.filter( (candidateRange) -> !candidateRange.overlapsAny(existingChunkRanges) )
				.max(Comparator.comparingLong(Range::length))
				.orElse(null);
	}

	protected static Optional<Range> chooseSmallestRangeContainingChanges(List<Range> rangeOptions, Set<Long> changeGroupingNumbersForRange) {
		Range smallestRangeContainingChanges = rangeOptions.stream()
				.filter( (candidateRange) -> rangeContainsAll(candidateRange, changeGroupingNumbersForRange) )
				.min(Comparator.comparingLong(Range::length))
				.get();  // this will throw an exception if there is none, should never happen because we chose changes based on the range
		return Optional.of(smallestRangeContainingChanges);
	}

	protected static boolean rangeContainsAll(Range range, Set<Long> changeGroupingNumbersForRange) {
		return changeGroupingNumbersForRange.stream()
				.allMatch( (groupingNumber) -> range.containsInclusive(groupingNumber) );
	}

	protected static Optional<Range> findMatchingRange(List<Range> options, Set<Range> existingChunkRanges) {
		Range matchingRange = options.stream()
				.filter( (candidateRange) -> existingChunkRanges.contains(candidateRange) )
				.findFirst()
				.orElse(null);
		return Optional.ofNullable(matchingRange);
	}
	
	@FunctionalInterface
	static interface PossibleChunkRangeCalculator {
		public List<Range> calculatePossibleChunkRangesForGroupingNumber(long groupingNumber);
	}
	
	@FunctionalInterface
	static interface ExistingChunkRangeFinder {
		public List<Range> findExistingChunkRanges();
	}

}
