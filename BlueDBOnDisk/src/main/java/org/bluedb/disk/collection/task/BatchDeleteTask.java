package org.bluedb.disk.collection.task;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.collection.BlueCollectionOnDisk;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.recovery.PendingBatchChange;
import org.bluedb.disk.recovery.RecoveryManager;

public class BatchDeleteTask<T extends Serializable> extends QueryTask {

	private final BlueCollectionOnDisk<T> collection;
	private final List<IndividualChange<T>> sortedChanges;

	public BatchDeleteTask(BlueCollectionOnDisk<T> collection, Collection<BlueKey> keys) {
		this.collection = collection;
		sortedChanges = new ArrayList<>();
		keys.forEach( (key) -> sortedChanges.add( IndividualChange.createDeleteChange(key)));
		Collections.sort(sortedChanges);
	}

	@Override
	public void execute() throws BlueDbException {
		RecoveryManager<T> recoveryManager = collection.getRecoveryManager();
		PendingBatchChange<T> batchChange = PendingBatchChange.createBatchUpsert(sortedChanges);
		recoveryManager.saveChange(batchChange);
		batchChange.apply(collection);
		recoveryManager.markComplete(batchChange);	
	}	

	@Override
	public String toString() {
		return "<" + getClass().getSimpleName() + " for " + sortedChanges.size() + ">";
	}

}
