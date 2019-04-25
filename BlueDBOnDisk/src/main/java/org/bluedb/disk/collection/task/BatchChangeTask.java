package org.bluedb.disk.collection.task;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.collection.BlueCollectionOnDisk;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.recovery.PendingBatchChange;
import org.bluedb.disk.recovery.RecoveryManager;

public class BatchChangeTask<T extends Serializable> extends QueryTask {

	private final BlueCollectionOnDisk<T> collection;
	private final List<IndividualChange<T>> sortedChanges;

	public BatchChangeTask(BlueCollectionOnDisk<T> collection, Map<BlueKey, T> values) {
		this.collection = collection;
		sortedChanges = toSortedChangeList(values);
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

	private static <T extends Serializable> List<IndividualChange<T>> toSortedChangeList(Map<BlueKey, T> values) {
		return values.entrySet().stream()
				.map( (e) -> IndividualChange.createInsertChange(e.getKey(), e.getValue()) )
				.sorted()
				.collect(Collectors.toList());
	}
}
