package org.bluedb.disk;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.bluedb.api.ReadOnlyBlueCollection;
import org.bluedb.api.ReadOnlyBlueDb;
import org.bluedb.api.ReadOnlyBlueTimeCollection;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.backup.BackupManager;
import org.bluedb.disk.collection.ReadOnlyBlueCollectionOnDisk;
import org.bluedb.disk.executors.BlueExecutor;
import org.bluedb.disk.file.FileUtils;

public class ReadOnlyBlueDbOnDisk implements ReadOnlyBlueDb {

	protected final Path path;
	protected final BackupManager backupManager;
	protected final BlueExecutor sharedExecutor;
	
	protected final Map<String, ReadOnlyBlueCollectionOnDisk<? extends Serializable>> collections = new HashMap<>();
	
	ReadOnlyBlueDbOnDisk(Path path) {
		this.path = path;
		this.backupManager = new BackupManager(this);
		this.sharedExecutor = new BlueExecutor(path.getFileName().toString());
	}
	
	//TODO: getCollection and getTimeCollection need to work even if they haven't called initialize or build. Return empty collection object if it doesn't exist
	//TODO: Should getTimeCollection throw an exception if the key ove the collection isn't a TimeKey?

	@Override
	public <T extends Serializable> ReadOnlyBlueCollection<T> getCollection(String name, Class<T> valueType) throws BlueDbException {
		synchronized(collections) {
			ReadOnlyBlueCollectionOnDisk<?> untypedCollection = collections.get(name);
			if (untypedCollection == null) {
				return null;
			}
			if (untypedCollection.getType().equals(valueType)) {
				@SuppressWarnings("unchecked")
				ReadOnlyBlueCollection<T> typedCollection = (ReadOnlyBlueCollection<T>) untypedCollection;
				return typedCollection;
			} else {
				throw new BlueDbException("Cannot cast BlueCollection<" + untypedCollection.getType() + "> to BlueCollection<" + valueType + ">");
			}
		}
	}

	@Override
	public <V extends Serializable> ReadOnlyBlueTimeCollection<V> getTimeCollection(String name, Class<V> valueType) throws BlueDbException {
		ReadOnlyBlueCollection<V> collection = getCollection(name, valueType);
		if(collection instanceof ReadOnlyBlueTimeCollection) {
			return (ReadOnlyBlueTimeCollection<V>) collection;
		} else {
			throw new BlueDbException("Cannot cast " + collection.getClass() + " to " + ReadOnlyBlueTimeCollection.class);
		}
	}

	@Override
	public void shutdown() {
		sharedExecutor.shutdown();
	}
	
	@Override
	public void shutdownNow() {
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

	protected List<ReadOnlyBlueCollectionOnDisk<?>> getAllCollectionsFromDisk() throws BlueDbException {
		List<File> subfolders = FileUtils.getSubFolders(path.toFile());
		List<ReadOnlyBlueCollectionOnDisk<?>> collections = Blutils.map(subfolders, (folder) -> getUntypedCollectionForBackup(folder.getName()));
		return collections;
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	protected ReadOnlyBlueCollectionOnDisk getUntypedCollectionForBackup(String folderName) throws BlueDbException {
		ReadOnlyBlueCollectionOnDisk collection;
		synchronized (collections) {
			collection = collections.get(folderName);
		}
		if (collection == null) {
			collection = new ReadOnlyBlueCollectionOnDisk(this, folderName, null, Serializable.class, Arrays.asList());
		}
		return collection;
	}
}
