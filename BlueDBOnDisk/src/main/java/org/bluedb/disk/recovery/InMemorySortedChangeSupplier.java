package org.bluedb.disk.recovery;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.bluedb.disk.segment.Range;

public class InMemorySortedChangeSupplier<T extends Serializable> implements SortedChangeSupplier<T> {
	private ArrayList<IndividualChange<T>> sortedChanges;
	
	private int currentIndex = -1;
	private int lastCheckpointIndex = -1;

	public InMemorySortedChangeSupplier(List<IndividualChange<T>> sortedChanges) {
		this.sortedChanges = new ArrayList<>(sortedChanges);
	}

	public InMemorySortedChangeSupplier(List<IndividualChange<T>> sortedChanges, Range initialRangeToSeekTo) {
		this.sortedChanges = new ArrayList<>(sortedChanges);
		seekToNextChangeInRange(initialRangeToSeekTo);
	}

	@Override
	public boolean seekToNextChangeInRange(Range range) {
		for(int i = currentIndex + 1; i < sortedChanges.size(); i++) {
			currentIndex = i;
			IndividualChange<T> change = sortedChanges.get(i);
			if(change.getKey().isInRange(range.getStart(), range.getEnd())) {
				return true;
			} else if(change.getKey().isAfterRange(range.getStart(), range.getEnd())) {
				return false; //The changes are sorted so once we are after the range we can stop looking.
			}
		}
		currentIndex = sortedChanges.size(); //No changes will be returned unless the cursor position is moved
		return false;
	}

	@Override
	public boolean hasMoreThanOneChangeLeftInRange(Range range) {
		int countFound = 0;
		for(int i = Math.max(0, currentIndex); i < sortedChanges.size(); i++) {
			IndividualChange<T> change = sortedChanges.get(i);
			if(change.getKey().isInRange(range.getStart(), range.getEnd())) {
				countFound++;
				if(countFound > 1) {
					return true;
				}
			} else if(change.getKey().isAfterRange(range.getStart(), range.getEnd())) {
				return false; //The changes are sorted so once we are after the range we can stop looking.
			}
		}
		return false;
	}

	@Override
	public Set<Long> findGroupingNumbersForNextChangesBeforeOrAtGroupingNumber(long maxGroupingNumber) {
		Set<Long> groupingNumbers = new HashSet<>();
		
		for(int i = Math.max(0, currentIndex); i < sortedChanges.size(); i++) {
			IndividualChange<T> change = sortedChanges.get(i);
			if(change.getKey().getGroupingNumber() <= maxGroupingNumber) {
				groupingNumbers.add(change.getKey().getGroupingNumber());
			} else {
				break; //The changes are sorted so once we are after the maxGroupingNumber we can stop looking.
			}
		}
		
		return groupingNumbers;
	}
	
	@Override
	public Optional<IndividualChange<T>> getNextChange() {
		if(currentIndex >= 0 & currentIndex < sortedChanges.size()) {
			return Optional.of(sortedChanges.get(currentIndex));
		}
		return Optional.empty();
	}
	
	@Override
	public void setCursorCheckpoint() {
		lastCheckpointIndex = currentIndex - 1; //Reset to one before so that if seek is called after reset then it'll find the current item
	}

	@Override
	public void setCursorToLastCheckpoint() {
		currentIndex = lastCheckpointIndex;
	}

	@Override
	public void setCursorToBeginning() {
		currentIndex = -1;
		lastCheckpointIndex = -1;
	}
}
