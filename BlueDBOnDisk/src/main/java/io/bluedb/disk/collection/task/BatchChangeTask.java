package io.bluedb.disk.collection.task;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.recovery.IndividualChange;
import io.bluedb.disk.recovery.PendingBatchChange;
import io.bluedb.disk.recovery.RecoveryManager;

public class BatchChangeTask<T extends Serializable> extends QueryTask {

	private final BlueCollectionOnDisk<T> collection;
	private final List<IndividualChange<T>> sortedChanges;

	public BatchChangeTask(BlueCollectionOnDisk<T> collection, Map<BlueKey, T> values) {
		this.collection = collection;
		sortedChanges = values.entrySet().stream()
				.map( (e) -> IndividualChange.insert(e.getKey(), e.getValue()) )
				.sorted()
				.collect(Collectors.toList());
	}

	@Override
	public void execute() throws BlueDbException {
		RecoveryManager<T> recoveryManager = collection.getRecoveryManager();	
		PendingBatchChange<T> change = PendingBatchChange.createBatchUpsert(sortedChanges);
		recoveryManager.saveChange(change);
		change.apply(collection);
		recoveryManager.markComplete(change);	
	}	

	@Override
	public String toString() {
		return "<" + getClass().getSimpleName() + " for " + sortedChanges.size() + ">";
	}
}
