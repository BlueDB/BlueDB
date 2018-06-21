package io.bluedb.disk;

import java.io.Serializable;
import java.nio.file.Path;

import io.bluedb.api.BlueCollection;
import io.bluedb.api.BlueDb;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.collection.BlueCollectionImpl;
import io.bluedb.disk.serialization.BlueSerializer;
import io.bluedb.disk.serialization.ThreadLocalFstSerializer;

public class BlueDbOnDisk implements BlueDb {

	private final Path path;
	private BlueSerializer serializer;
	
	BlueDbOnDisk(Path path, Class<?>...registeredSerializableClasses) {
		this.path = path;
		this.serializer = new ThreadLocalFstSerializer(registeredSerializableClasses);
	}

	@Override
	public <T extends Serializable> BlueCollection<T> getCollection(Class<T> type, String name) {
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
	
	public BlueSerializer getSerializer() {
		return serializer;
	}

	private void recover() {
		// TODO implement and also add to startup
	}
}
