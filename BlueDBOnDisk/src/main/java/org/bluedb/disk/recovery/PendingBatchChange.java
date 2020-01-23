package org.bluedb.disk.recovery;

import java.io.Serializable;
import java.util.List;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.BatchUtils;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.segment.ReadWriteSegmentManager;

public class PendingBatchChange<T extends Serializable> implements Serializable, Recoverable<T> {

	private static final long serialVersionUID = 1L;

	private List<IndividualChange<T>> sortedChanges;
	private long timeCreated;
	private long recoverableId;
	
	private PendingBatchChange(List<IndividualChange<T>> sortedChanges) {
		this.sortedChanges = sortedChanges;
		timeCreated = System.currentTimeMillis();
	}

	public static <T extends Serializable> PendingBatchChange<T> createBatchChange(List<IndividualChange<T>> sortedChanges){
		return new PendingBatchChange<T>(sortedChanges);
	}

	@Override
	public void apply(ReadWriteCollectionOnDisk<T> collection) throws BlueDbException {
		ReadWriteSegmentManager<T> segmentManager = collection.getSegmentManager();
		BatchUtils.apply(segmentManager, sortedChanges);
		collection.getIndexManager().addToAllIndexes(sortedChanges);
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
