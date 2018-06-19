package io.bluedb.disk;

import java.io.Serializable;
import io.bluedb.api.BlueDb;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.collection.BlueCollectionImpl;
import io.bluedb.disk.recovery.RecoveryManager;
import io.bluedb.api.BlueCollection;

public class BlueDbOnDisk implements BlueDb {

	// TODO figure out directory

	@Override
	public <T extends Serializable> BlueCollection<T> getCollection(Class<T> type) {
		// TODO make sure only one Collection object per collection to avoid concurrency issues
		return new BlueCollectionImpl<>(type);
	}

	@Override
	public void shutdown() throws BlueDbException {
		// TODO Auto-generated method stub
	}

	public String getPath() {
		// TODO
		return null;
	}

	private void recover() {
		// TODO implement and also add to startup
		RecoveryManager.getPendingChanges();
	}
}
