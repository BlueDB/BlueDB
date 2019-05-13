package org.bluedb.disk.collection.task;

import java.io.Serializable;
import java.util.NoSuchElementException;

import org.bluedb.api.Mapper;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.collection.BlueCollectionOnDisk;
import org.bluedb.disk.recovery.PendingChange;
import org.bluedb.disk.recovery.RecoveryManager;
import org.bluedb.disk.serialization.BlueSerializer;

public class ReplaceTask<T extends Serializable> extends QueryTask {
	private final BlueCollectionOnDisk<T> collection;
	private final BlueKey key;
	private final Mapper<T> mapper;

	public ReplaceTask(BlueCollectionOnDisk<T> collection, BlueKey key, Mapper<T> updater) {
		this.collection = collection;
		this.key = key;
		this.mapper = updater;
	}

	@Override
	public void execute() throws BlueDbException {
		BlueSerializer serializer = collection.getSerializer();
		RecoveryManager<T> recoveryManager = collection.getRecoveryManager();
		T value = collection.get(key);
		if (value == null) {
			throw new NoSuchElementException("Cannot find object for key: " + key.toString());
		}
		PendingChange<T> change;
		try {
			change = PendingChange.createUpdate(key, value, mapper, serializer);
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
