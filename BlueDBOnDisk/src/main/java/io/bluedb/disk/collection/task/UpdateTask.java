package io.bluedb.disk.collection.task;

import java.io.Serializable;
import io.bluedb.api.Updater;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.recovery.PendingChange;
import io.bluedb.disk.recovery.RecoveryManager;
import io.bluedb.disk.serialization.BlueSerializer;

public class UpdateTask<T extends Serializable> extends QueryTask {
	private final BlueCollectionOnDisk<T> collection;
	private final BlueKey key;
	private final Updater<T> updater;

	public UpdateTask(BlueCollectionOnDisk<T> collection, BlueKey key, Updater<T> updater) {
		this.collection = collection;
		this.key = key;
		this.updater = updater;
	}

	@Override
	public void execute() throws BlueDbException {
		BlueSerializer serializer = collection.getSerializer();
		RecoveryManager<T> recoveryManager = collection.getRecoveryManager();
		T value = collection.get(key);
		PendingChange<T> change;
		try {
			change = PendingChange.createUpdate(key, value, updater, serializer);
		} catch(Throwable t) {
			t.printStackTrace();
			throw new BlueDbException("Error updating value", t);
		}
		recoveryManager.saveChange(change);
		change.apply(collection);
		recoveryManager.markComplete(change);
	}

	@Override
	public String toString() {
		return "<UpdateTask for key " + key + ">";
	}
}
