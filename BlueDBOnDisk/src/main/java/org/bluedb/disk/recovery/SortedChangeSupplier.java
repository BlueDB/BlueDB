package org.bluedb.disk.recovery;

import java.io.Serializable;
import java.util.Optional;
import java.util.Set;

import org.bluedb.disk.segment.Range;

/**
 * A sorted change supplier allows a batch update process to apply many updates without having to load
 * all of the changes into memory at one time. The supplier has a cursor that can be moved by calling seek
 * methods to search in order for the next changes you want to process.
 * @param <T> - The object type that is being updated.
 */
public interface SortedChangeSupplier<T extends Serializable> {
	
	/**
	 * Skips the change at the current cursor location and reads through the changes in order until it finds the next
	 * change in the given range. The cursor will be pointing at the change it found. If no change is found then the cursor
	 * position will point at the first change outside the given range. If it reaches the end of the changes then the cursor
	 * position will need to changed in order to return any changes.
	 * @param range - Lets you specify a range for the next change that you are looking for.
	 * @return true if it found and is pointing at the next change in the given range, false if not.
	 */
	public boolean seekToNextChangeInRange(Range range);
	
	/**
	 * @return the change that the cursor is currently pointing at without changing the cursor's position. Returns
	 * empty if the cursor is not currently pointing at a change.
	 */
	public Optional<IndividualChange<T>> getNextChange();
	
	/**
	 * 
	 * @param range - The range to check. 
	 * @return true if the change at the current cursor position overlaps with the given range. This operation
	 * does not move the cursor position.
	 */
	public default boolean nextChangeOverlapsRange(Range range) {
		Optional<IndividualChange<T>> nextChange = getNextChange();
		return nextChange.isPresent() && nextChange.get().overlaps(range);
	}

	/**
	 * Checks if there is more than one change left in the given range, including the change at the current cursor
	 * location. This operation does NOT move the cursor.
	 * @param range - The range to check.
	 * @return true if more than one change is left in the given range. Else false.
	 */
	public boolean hasMoreThanOneChangeLeftInRange(Range range);

	/**
	 * Finds all of the grouping numbers for the remaining changes before or at the given maxGroupingNumber, including
	 * the change at the current cursor location. This operation does NOT move the cursor.
	 * @param maxGroupingNumber - Only change grouping numbers less than or equal to this number will be returned.
	 * @return a all of the grouping numbers for the remaining changes before or at the given 
	 * maxGroupingNumber, including the change at the current cursor location.
	 */
	public Set<Long> findGroupingNumbersForNextChangesBeforeOrAtGroupingNumber(long maxGroupingNumber);

	/**
	 * Saves a cursor position that will result in a seek starting to search from the change at the current cursor position.
	 * This allows you to come back to this position later.
	 */
	public void setCursorCheckpoint();
	
	/**
	 * Sets the cursor position to the last checkpoint. If no checkpoint has been set then the next seek
	 * will start searching from the beginning of the changes.
	 */
	public void setCursorToLastCheckpoint();
	
	/**
	 * Resets the cursor position so that the next seek will start searching from the beginning of the changes.
	 */
	public void setCursorToBeginning();
}
