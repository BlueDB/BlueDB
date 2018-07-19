package io.bluedb.disk;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.bluedb.api.BlueCollection;
import io.bluedb.api.BlueDb;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.backup.BackupTask;
import io.bluedb.disk.collection.BlueCollectionOnDisk;

public class BlueDbOnDisk implements BlueDb {

	private final Path path;
	
	private final Map<String, BlueCollectionOnDisk<? extends Serializable>> collections = new HashMap<>();
	
	BlueDbOnDisk(Path path, Class<?>...registeredSerializableClasses) {
		this.path = path;
	}

	@Override
	public <T extends Serializable> BlueCollection<T> getCollection(Class<T> type, String name) throws BlueDbException {
		synchronized (collections) {
			@SuppressWarnings("unchecked")
			BlueCollectionOnDisk<T> collection = (BlueCollectionOnDisk<T>) collections.get(name);
			if(collection == null) {
				collection = new BlueCollectionOnDisk<>(this, name, type);
				collections.put(name, collection);
			} else if(!collection.getType().equals(type)) {
				throw new BlueDbException("The " + name + " collection already exists for a different type [collectionType=" + collection.getType() + " invalidType=" + type + "]");
			}
				
			return collection;
		}
	}

	@Override
	public void backup(Path path) throws BlueDbException {
		BackupTask backupTask = new BackupTask(this, path);
		List<BlueCollectionOnDisk<?>> collectionsToBackup = new ArrayList<BlueCollectionOnDisk<?>>(collections.values());
		backupTask.backup(collectionsToBackup);
	}

	@Override
	public void shutdown() throws BlueDbException {
		for (BlueCollection<?> collection: collections.values()) {
			BlueCollectionOnDisk<?> diskCollection = (BlueCollectionOnDisk<?>) collection;
			diskCollection.shutdown();
		}
	}

	public Path getPath() {
		return path;
	}
	
}
