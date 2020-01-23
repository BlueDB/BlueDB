package org.bluedb.disk.collection.task;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.query.QueryOnDisk;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.recovery.PendingBatchChange;
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
		List<BlueEntity<T>> entities = query.getEntities();
		List<IndividualChange<T>> sortedChanges = createSortedChangeList(entities);
		PendingBatchChange<T> change = PendingBatchChange.createBatchChange(sortedChanges);
		recoveryManager.saveChange(change);
		change.apply(collection);
		recoveryManager.markComplete(change);
	}

	protected static <T extends Serializable> List<IndividualChange<T>> createSortedChangeList(List<BlueEntity<T>> entities) {
		return entities.stream()
				.map((e) -> new IndividualChange<>(e.getKey(), e.getValue(), null))
				.sorted()
				.collect(Collectors.toList());
	}

	@Override
	public String toString() {
		return "<DeleteMultipleTask on query " + query.toString() + ">";
	}
}
