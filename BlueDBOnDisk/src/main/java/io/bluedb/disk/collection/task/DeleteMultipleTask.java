package io.bluedb.disk.collection.task;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.query.BlueQueryOnDisk;
import io.bluedb.disk.recovery.PendingChange;
import io.bluedb.disk.recovery.RecoveryManager;
import io.bluedb.disk.serialization.BlueEntity;

public class DeleteMultipleTask<T extends Serializable> extends QueryTask {
	private final BlueCollectionOnDisk<T> collection;
	BlueQueryOnDisk<T> query;
	
	public DeleteMultipleTask(BlueCollectionOnDisk<T> collection, BlueQueryOnDisk<T> query) {
		this.collection = collection;
		this.query = query;
	}

	@Override
	public void execute() throws BlueDbException {
		RecoveryManager<T> recoveryManager = collection.getRecoveryManager();
		List<BlueEntity<T>> entities = query.getEntities();
		List<PendingChange<T>> changes = createDeletePendingChanges(entities);
		for (PendingChange<T> change: changes) {
			recoveryManager.saveChange(change);
			change.apply(collection);
			recoveryManager.markComplete(change);
		}
	}

	private List<PendingChange<T>> createDeletePendingChanges(List<BlueEntity<T>> entities) {
		List<PendingChange<T>> changes = new ArrayList<>();
		for (BlueEntity<T> entity: entities) {
			BlueKey key = entity.getKey();
			T value = entity.getValue();
			PendingChange<T> delete = PendingChange.createDelete(key, value);
			changes.add(delete);
		}
		return changes;
	}

	@Override
	public String toString() {
		return "<DeleteMultipleTask on query " + query.toString() + ">";
	}
}
