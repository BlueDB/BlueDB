package io.bluedb.disk;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import io.bluedb.api.BlueDb;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.collection.BlueCollectionImpl;
import io.bluedb.disk.recovery.RecoveryManager;
import io.bluedb.api.BlueCollection;

public class BlueDbOnDisk implements BlueDb {

	final Path path;
	// TODO figure out directory
	
	public BlueDbOnDisk() {
		path = Paths.get(".", "bluedb");
	}

	@Override
	public <T extends Serializable> BlueCollection<T> getCollection(Class<T> type) {
		// TODO make sure only one Collection object per collection to avoid concurrency issues
		return new BlueCollectionImpl<>(this, type);
	}

	@Override
	public void shutdown() throws BlueDbException {
		// TODO Auto-generated method stub
	}

	public Path getPath() {
		return path;
	}

	private void recover() {
		// TODO implement and also add to startup
		RecoveryManager.getPendingChanges();
	}
}
