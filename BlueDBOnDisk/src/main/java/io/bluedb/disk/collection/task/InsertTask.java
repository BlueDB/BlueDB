package io.bluedb.disk.collection.task;

import java.io.Serializable;
import io.bluedb.api.exceptions.DuplicateKeyException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.collection.BlueCollectionImpl;
import io.bluedb.disk.recovery.PendingChange;
import io.bluedb.disk.recovery.RecoveryManager;

public class InsertTask<T extends Serializable> implements Runnable {

	private final BlueCollectionImpl<T> collection;
	private final BlueKey key;
	private final T value;
	
	public InsertTask(BlueCollectionImpl<T> collection, BlueKey key, T value) {
		this.collection = collection;
		this.key = key;
		this.value = value;
	}

	@Override
	public void run() {
		try {
			RecoveryManager<T> recoveryManager = collection.getRecoveryManager();
			if (collection.contains(key)) {
				throw new DuplicateKeyException("key already exists: " + key);
			}
			PendingChange<T> change = recoveryManager.saveInsert(key, value);
			collection.applyChange(change);
			recoveryManager.removeChange(change);
		} catch (Throwable t) {
			// TODO rollback or try again?
			t.printStackTrace();
			throw new RuntimeException(); // TODO
		} finally {
		}
	}

	@Override
	public String toString() {
		return "<InsertTask for key " + key + " and value " + value + ">";
	}
}
