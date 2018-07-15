package io.bluedb.disk.collection.task;

import java.io.Serializable;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.exceptions.DuplicateKeyException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.recovery.PendingChange;
import io.bluedb.disk.recovery.RecoveryManager;
import io.bluedb.disk.serialization.BlueSerializer;

public class InsertTask<T extends Serializable> extends QueryTask {

	private final BlueCollectionOnDisk<T> collection;
	private final BlueKey key;
	private final T value;
	
	public InsertTask(BlueCollectionOnDisk<T> collection, BlueKey key, T value) {
		this.collection = collection;
		this.key = key;
		this.value = value;
	}

	@Override
	public void execute() throws BlueDbException {
		BlueSerializer serializer = collection.getSerializer();
		RecoveryManager<T> recoveryManager = collection.getRecoveryManager();
		if (collection.contains(key)) {
			throw new DuplicateKeyException("key already exists", key);
		}
		PendingChange<T> change = PendingChange.createInsert(key, value, serializer);
		recoveryManager.saveChange(change);
		change.apply(collection);
		recoveryManager.removeChange(change);
	}

	@Override
	public String toString() {
		return "<InsertTask for key " + key + " and value " + value + ">";
	}
}
