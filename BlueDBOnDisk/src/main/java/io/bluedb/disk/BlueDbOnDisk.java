package io.bluedb.disk;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import io.bluedb.api.BlueCollection;
import io.bluedb.api.BlueDb;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.collection.BlueCollectionImpl;

public class BlueDbOnDisk implements BlueDb {

	private final Path path;
	
	private final Map<String, BlueCollection<?>> collections = new HashMap<>();
	
	BlueDbOnDisk(Path path, Class<?>...registeredSerializableClasses) {
		this.path = path;
	}

	@Override
	public <T extends Serializable> BlueCollection<T> getCollection(Class<T> type, String name) throws BlueDbException {
		synchronized (collections) {
			String key = type.getName() + "-" + name;
			@SuppressWarnings("unchecked")
			BlueCollection<T> collection = (BlueCollection<T>) collections.get(key);
			if(collection == null) {
				collection = new BlueCollectionImpl<>(this, name, type);
				collections.put(key, collection);
			}
			return collection;
		}
	}

	@Override
	public void shutdown() throws BlueDbException {
		for (BlueCollection<?> collection: collections.values()) {
			BlueCollectionImpl<?> diskCollection = (BlueCollectionImpl<?>) collection;
			diskCollection.shutdown();
		}
	}

	public Path getPath() {
		return path;
	}
}
