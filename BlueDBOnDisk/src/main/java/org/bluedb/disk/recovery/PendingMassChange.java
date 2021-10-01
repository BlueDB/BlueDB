package org.bluedb.disk.recovery;

import java.io.Serializable;
import java.nio.file.Path;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.segment.ReadWriteSegmentManager;

public class PendingMassChange<T extends Serializable> implements Serializable, Recoverable<T> {

	private static final long serialVersionUID = 1L;

	private long timeCreated;
	private long recoverableId;
	private Path changesFilePath;
	
	public PendingMassChange(long timeCreated, long recoverableId, Path changesFilePath) {
		this.timeCreated = timeCreated;
		this.recoverableId = recoverableId;
		this.changesFilePath = changesFilePath;
	}

	@Override
	public void apply(ReadWriteCollectionOnDisk<T> collection) throws BlueDbException {
		try(SortedChangeSupplier<T> sortedChangeSupplier = new OnDiskSortedChangeSupplier<>(changesFilePath, collection.getFileManager())) {
			ReadWriteSegmentManager<T> segmentManager = collection.getSegmentManager();
			segmentManager.applyChanges(sortedChangeSupplier);
			collection.getIndexManager().indexChanges(sortedChangeSupplier);
		}
	}

	@Override
	public long getTimeCreated() {
		return timeCreated;
	}

	@Override
	public String toString() {
		return "<" + this.getClass().getSimpleName() + " for " + changesFilePath + ">";
	}

	@Override
	public long getRecoverableId() {
		return recoverableId;
	}

	@Override
	public void setRecoverableId(long recoverableId) {
		this.recoverableId = recoverableId;
	}
	
	public Path getChangesFilePath() {
		return changesFilePath;
	}
}
