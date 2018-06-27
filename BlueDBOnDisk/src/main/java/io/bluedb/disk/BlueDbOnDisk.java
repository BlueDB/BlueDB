package io.bluedb.disk;

import java.io.Serializable;
import java.nio.file.Path;

import io.bluedb.api.BlueCollection;
import io.bluedb.api.BlueDb;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.collection.BlueCollectionImpl;

public class BlueDbOnDisk implements BlueDb {

	private final Path path;
	
	BlueDbOnDisk(Path path, Class<?>...registeredSerializableClasses) {
		this.path = path;
	}

	@Override
	public <T extends Serializable> BlueCollection<T> getCollection(Class<T> type, String name) throws BlueDbException {
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
	}
}
