package org.bluedb.disk.recovery;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadWriteSegmentManager;

public class PendingBatchChange<T extends Serializable> implements Serializable, Recoverable<T> {

	private static final long serialVersionUID = 1L;

	private List<IndividualChange<T>> sortedChanges;
	private long timeCreated;
	private long recoverableId;
	
	private PendingBatchChange(List<IndividualChange<T>> sortedChanges) {
		this.sortedChanges = sortedChanges != null ? new LinkedList<>(sortedChanges) : new LinkedList<>();
		timeCreated = System.currentTimeMillis();
	}

	public static <T extends Serializable> PendingBatchChange<T> createBatchChange(List<IndividualChange<T>> sortedChanges){
		return new PendingBatchChange<T>(sortedChanges);
	}

	@Override
	public void apply(ReadWriteCollectionOnDisk<T> collection) throws BlueDbException {
		SortedChangeSupplier<T> sortedChangeSupplier = new InMemorySortedChangeSupplier<T>(sortedChanges);
		ReadWriteSegmentManager<T> segmentManager = collection.getSegmentManager();
		segmentManager.applyChanges(sortedChangeSupplier);
		collection.getIndexManager().indexChanges(sortedChanges);
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
	
	public void removeChangesOutsideRange(Range groupingNumberRange) {
		Iterator<IndividualChange<T>> it = sortedChanges.iterator();
		while(it.hasNext()) {
			IndividualChange<T> change = it.next();
			if(!change.getKey().isInRange(groupingNumberRange.getStart(), groupingNumberRange.getEnd())) {
				it.remove();
			}
		}
	}
	
	public boolean isEmpty() {
		return sortedChanges.isEmpty();
	}
	
	public List<IndividualChange<T>> getSortedChanges() {
		return sortedChanges;
	}
}
