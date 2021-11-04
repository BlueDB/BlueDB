package org.bluedb.disk.collection.task;

import java.io.Serializable;
import java.util.Iterator;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.recovery.PendingMassChange;
import org.bluedb.disk.recovery.RecoveryManager;

public class BatchIteratorChangeTask<T extends Serializable> extends QueryTask {
	
	private final ReadWriteCollectionOnDisk<T> collection;
	private final Iterator<IndividualChange<T>> changeIterator;

	public BatchIteratorChangeTask(String description, ReadWriteCollectionOnDisk<T> collection, Iterator<IndividualChange<T>> changeIterator) {
		super(description);
		this.collection = collection;
		this.changeIterator = changeIterator;
	}

	@Override
	public void execute() throws BlueDbException {
		RecoveryManager<T> recoveryManager = collection.getRecoveryManager();
		PendingMassChange<T> changeBatch = recoveryManager.saveMassChange(changeIterator);
		changeBatch.apply(collection);
		recoveryManager.markComplete(changeBatch);
	}
}
