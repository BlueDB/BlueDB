package org.bluedb.disk.collection.task;

import java.io.Serializable;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.query.QueryOnDisk;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.recovery.PendingMassChange;
import org.bluedb.disk.recovery.RecoveryManager;
import org.bluedb.disk.serialization.BlueEntity;

public class DeleteMultipleTask<T extends Serializable> extends QueryTask {
	private final ReadWriteCollectionOnDisk<T> collection;
	QueryOnDisk<T> query;
	
	public DeleteMultipleTask(ReadWriteCollectionOnDisk<T> collection, QueryOnDisk<T> query) {
		this.collection = collection;
		this.query = query;
	}

	@Override
	public void execute() throws BlueDbException {
		RecoveryManager<T> recoveryManager = collection.getRecoveryManager();
		PendingMassChange<T> changeBatch = recoveryManager.saveMassChange(query, DeleteMultipleTask::createChange);
		changeBatch.apply(collection);
		recoveryManager.markComplete(changeBatch);
	}

	protected static <T extends Serializable> IndividualChange<T> createChange(BlueEntity<T> entity) {
		return new IndividualChange<>(entity.getKey(), entity.getValue(), null);
	}

	@Override
	public String toString() {
		return "<DeleteMultipleTask on query " + query.toString() + ">";
	}
}
