package io.bluedb.disk;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import io.bluedb.zip.ZipUtils;

public class BlueDbOnDisk implements BlueDb {

	private final Path path;
	
	private final Map<String, BlueCollectionOnDisk<? extends Serializable>> collections = new HashMap<>();
	
	BlueDbOnDisk(Path path, Class<?>...registeredSerializableClasses) {
		this.path = path;
	}

	@Override
	public <T extends Serializable> BlueCollection<T> getCollection(Class<T> type, Class<? extends BlueKey> keyType, String name) throws BlueDbException {
		synchronized (collections) {
			@SuppressWarnings("unchecked")
			BlueCollectionOnDisk<T> collection = (BlueCollectionOnDisk<T>) collections.get(name);
			if(collection == null) {
				collection = new BlueCollectionOnDisk<T>(this, name, type, keyType);
				collections.put(name, collection);
			} else if(!collection.getType().equals(type)) {
				throw new BlueDbException("The " + name + " collection already exists for a different type [collectionType=" + collection.getType() + " invalidType=" + type + "]");
			}
				
			return collection;
		}
	}

	@Override
	public void backup(Path zipPath) throws BlueDbException {
		try {
			Path tempDirectoryPath = Files.createTempDirectory("bluedb_backup_in_progress");
			tempDirectoryPath.toFile().deleteOnExit();
			Path unzippedBackupPath = Paths.get(tempDirectoryPath.toString(), "bluedb");
			BackupTask backupTask = new BackupTask(this, unzippedBackupPath);
			List<BlueCollectionOnDisk<?>> collectionsToBackup = getAllCollectionsFromDisk();
			backupTask.backup(collectionsToBackup);
			ZipUtils.zipFile(unzippedBackupPath, zipPath);
			tempDirectoryPath.toFile().delete();
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
		List<BlueCollectionOnDisk<?>> collections = Blutils.map(subfolders, (folder) -> getCollection(folder.getName()));
		return collections;
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	protected BlueCollectionOnDisk getCollection(String folderName) throws BlueDbException {
		synchronized (collections) {
			BlueCollectionOnDisk collection = collections.get(folderName);
			if (collection == null) {
				collection = new BlueCollectionOnDisk(this, folderName, Serializable.class, BlueKey.class);
			}
			return collection;
		}
	}
}
