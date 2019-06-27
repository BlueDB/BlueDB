package org.bluedb.disk;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.BlueDb;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.backup.BackupManager;
import org.bluedb.disk.collection.BlueCollectionOnDisk;
import org.bluedb.disk.executors.BlueExecutor;
import org.bluedb.disk.file.FileUtils;

public class BlueDbOnDisk implements BlueDb {

	private final Path path;
	private final BackupManager backupManager;
	private final BlueExecutor sharedExecutor;
	
	private final Map<String, BlueCollectionOnDisk<? extends Serializable>> collections = new HashMap<>();
	
	BlueDbOnDisk(Path path, Class<?>...registeredSerializableClasses) {
		this.path = path;
		this.backupManager = new BackupManager(this);
		this.sharedExecutor = new BlueExecutor(path.getFileName().toString());
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
	public <T extends Serializable, K extends BlueKey> BlueCollectionOnDiskBuilder<T> collectionBuilder(String name, Class <K> keyType, Class<T> valueType) {
		return new BlueCollectionOnDiskBuilder<T>(this, name, keyType, valueType);
	}

	@Deprecated
	@Override
	public <T extends Serializable> BlueCollection<T> initializeCollection(String name, Class<? extends BlueKey> keyType, Class<T> valueType, @SuppressWarnings("unchecked") Class<? extends Serializable>... additionalClassesToRegister) throws BlueDbException {
		return initializeCollection(name, keyType, valueType, Arrays.asList(additionalClassesToRegister));
	}

	protected <T extends Serializable> BlueCollection<T> initializeCollection(String name, Class<? extends BlueKey> keyType, Class<T> valueType, List<Class<? extends Serializable>> additionalClassesToRegister) throws BlueDbException {
		synchronized (collections) {
			@SuppressWarnings("unchecked")
			BlueCollectionOnDisk<T> collection = (BlueCollectionOnDisk<T>) collections.get(name);
			if(collection == null) {
				collection = new BlueCollectionOnDisk<T>(this, name, keyType, valueType, additionalClassesToRegister);
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
			List<BlueCollectionOnDisk<?>> collectionsToBackup = getAllCollectionsFromDisk();
			backupManager.backup(collectionsToBackup, zipPath);
		} catch (IOException | BlueDbException e) {
			throw new BlueDbException("BlueDB backup failed", e);
		}
	}

	@Override
	public void shutdown() throws BlueDbException {
		sharedExecutor.shutdown();
	}
	
	@Override
	public void shutdownNow() throws BlueDbException {
		sharedExecutor.shutdownNow();
	}
	
	@Override
	public boolean awaitTermination(long timeout, TimeUnit timeUnit) throws BlueDbException {
		try {
			return sharedExecutor.awaitTermination(timeout, timeUnit);
		} catch(Throwable t) {
			throw new BlueDbException("Failure during shutdown", t);
		}
	}

	public Path getPath() {
		return path;
	}

	public BackupManager getBackupManager() {
		return backupManager;
	}

	public BlueExecutor getSharedExecutor() {
		return sharedExecutor;
	}

	protected List<BlueCollectionOnDisk<?>> getAllCollectionsFromDisk() throws BlueDbException {
		List<File> subfolders = FileUtils.getFolderContents(path.toFile(), (f) -> f.isDirectory());
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
			collection = new BlueCollectionOnDisk(this, folderName, null, Serializable.class, Arrays.asList());
		}
		return collection;
	}
}
