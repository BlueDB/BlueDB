package org.bluedb.disk.recovery;

import java.io.Serializable;
import java.util.Iterator;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.segment.Range;

public class SortedChangeIterator<T extends Serializable> implements Iterator<IndividualChange<T>> {
	
	private SortedChangeSupplier<T> sortedChangeSupplier;
	
	public SortedChangeIterator(SortedChangeSupplier<T> sortedChangeSupplier) throws BlueDbException {
		this.sortedChangeSupplier = sortedChangeSupplier;
		this.sortedChangeSupplier.setCursorToBeginning();
		sortedChangeSupplier.seekToNextChangeInRange(Range.createMaxRange()); //Seek to the first item
	}

	@Override
	public boolean hasNext() {
		try {
			return sortedChangeSupplier.getNextChange().isPresent();
		} catch (BlueDbException e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public IndividualChange<T> next() {
		try {
			IndividualChange<T> next = sortedChangeSupplier.getNextChange().orElse(null);
			sortedChangeSupplier.seekToNextChangeInRange(Range.createMaxRange());
			return next;
		} catch (BlueDbException e) {
			e.printStackTrace();
			return null;
		}
	}

}
