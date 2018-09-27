package io.bluedb.disk.collection.task;

import java.io.Serializable;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.recovery.PendingChange;
import io.bluedb.disk.recovery.RecoveryManager;

public class DeleteTask<T extends Serializable> extends QueryTask {
	private final BlueCollectionOnDisk<T> collection;
	private final BlueKey key;
	
	public DeleteTask(BlueCollectionOnDisk<T> collection, BlueKey key) {
		this.collection = collection;
		this.key = key;
	}

	@Override
	public void execute() throws BlueDbException {
		RecoveryManager<T> recoveryManager = collection.getRecoveryManager();
		T value = collection.get(key);
		PendingChange<T> change = PendingChange.createDelete(key, value);
		recoveryManager.saveChange(change);
		change.apply(collection);
		recoveryManager.markComplete(change);
	}

	@Override
	public String toString() {
		return "<DeleteTask for key " + key + ">";
	}
}
