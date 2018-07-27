package io.bluedb.disk;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.bluedb.api.BlueCollection;
import io.bluedb.api.BlueDb;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.backup.BackupTask;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.file.FileManager;

public class BlueDbOnDisk implements BlueDb {

	private final Path path;
	
	private final Map<String, BlueCollectionOnDisk<? extends Serializable>> collections = new HashMap<>();
	
	BlueDbOnDisk(Path path, Class<?>...registeredSerializableClasses) {
		this.path = path;
	}

	@Override
	public <T extends Serializable> BlueCollection<T> getCollection(String name, Class<T> valueType) throws BlueDbException {
		synchronized(collections) {
			BlueCollectionOnDisk<?> untypedCollection = collections.get(name);
			if (untypedCollection == null) {
				return null;
			}
			if (untypedCollection.getType().equals(valueType)) {
				@SuppressWarnings("unchecked")
				BlueCollection<T> typedCollection = (BlueCollection<T>) untypedCollection;
				return typedCollection;
			} else {
				throw new BlueDbException("Cannot cast BlueCollection<" + untypedCollection.getType() + "> to BlueCollection<" + valueType + ">");
			}
		}
	}

	@Override
	public <T extends Serializable> BlueCollection<T> initializeCollection(String name, Class<? extends BlueKey> keyType, Class<T> valueType, Class<?>... additionalClassesToRegister) throws BlueDbException {
		synchronized (collections) {
			@SuppressWarnings("unchecked")
			BlueCollectionOnDisk<T> collection = (BlueCollectionOnDisk<T>) collections.get(name);
			if(collection == null) {
				collection = new BlueCollectionOnDisk<T>(this, name, keyType, valueType);
				collections.put(name, collection);
			} else if(!collection.getType().equals(valueType)) {
				throw new BlueDbException("The " + name + " collection already exists for a different type [collectionType=" + collection.getType() + " invalidType=" + valueType + "]");
			} else if (!collection.getKeyType().equals(keyType)) {
				throw new BlueDbException("The " + name + " collection already exists for a different key type (" + collection.getKeyType() + ") vs " + keyType);
			}

		return collection;
		}
	}

	@Override
	public void backup(Path zipPath) throws BlueDbException {
		try {
			BackupTask backupTask = new BackupTask(this, zipPath);
			List<BlueCollectionOnDisk<?>> collectionsToBackup = getAllCollectionsFromDisk();
			backupTask.backup(collectionsToBackup);
		} catch (IOException | BlueDbException e) {
			e.printStackTrace();
			throw new BlueDbException("BlueDB backup failed", e);
		}
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

	protected List<BlueCollectionOnDisk<?>> getAllCollectionsFromDisk() throws BlueDbException {
		List<File> subfolders = FileManager.getFolderContents(path.toFile(), (f) -> f.isDirectory());
		List<BlueCollectionOnDisk<?>> collections = Blutils.map(subfolders, (folder) -> getUntypedCollectionForBackup(folder.getName()));
		return collections;
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	protected BlueCollectionOnDisk getUntypedCollectionForBackup(String folderName) throws BlueDbException {
		BlueCollectionOnDisk collection;
		synchronized (collections) {
			collection = collections.get(folderName);
		}
		if (collection == null) {
			collection = new BlueCollectionOnDisk(this, folderName, null, Serializable.class);
		}
		return collection;
	}
}
