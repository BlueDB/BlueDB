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
import org.bluedb.api.BlueCollectionBuilder;
import org.bluedb.api.BlueDb;
import org.bluedb.api.BlueTimeCollection;
import org.bluedb.api.BlueTimeCollectionBuilder;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.backup.BackupManager;
import org.bluedb.disk.collection.BlueCollectionOnDisk;
import org.bluedb.disk.collection.BlueTimeCollectionOnDisk;
import org.bluedb.disk.collection.ReadableBlueCollectionOnDisk;
import org.bluedb.disk.executors.BlueExecutor;
import org.bluedb.disk.file.FileUtils;
import org.bluedb.disk.segment.SegmentSizeSetting;

public class BlueDbOnDisk extends ReadOnlyBlueDbOnDisk implements BlueDb {

	protected final BackupManager backupManager;
	protected final BlueExecutor sharedExecutor;
	private final Map<String, BlueCollectionOnDisk<? extends Serializable>> collections = new HashMap<>();
	

	public BlueDbOnDisk(Path path) {
		super(path);
		this.backupManager = new BackupManager(this);
		this.sharedExecutor = new BlueExecutor(path.getFileName().toString());
	}

	@Override
	@Deprecated
	public <K extends BlueKey, T extends Serializable> BlueCollectionBuilder<K, T> collectionBuilder(String name, Class <K> keyType, Class<T> valueType) {
		return getCollectionBuilder(name, keyType, valueType);
	}

	@Override
	public <K extends BlueKey, V extends Serializable> BlueCollectionBuilder<K, V> getCollectionBuilder(String name, Class<K> keyType, Class<V> valueType) {
		return new BlueCollectionOnDiskBuilder<>(this, name, keyType, valueType);
	}

	@Override
	public <K extends BlueKey, V extends Serializable> BlueTimeCollectionBuilder<K, V> getTimeCollectionBuilder(String name, Class<K> keyType, Class<V> valueType) {
		return new BlueTimeCollectionOnDiskBuilder<>(this, name, keyType, valueType); //TODO: Should this restrict key type to only TimeKey and/or throw exception if it isn't a TimeKey?
	}

	@Deprecated
	@Override
	public <T extends Serializable> BlueCollection<T> initializeCollection(String name, Class<? extends BlueKey> keyType, Class<T> valueType, @SuppressWarnings("unchecked") Class<? extends Serializable>... additionalClassesToRegister) throws BlueDbException {
		return initializeCollection(name, keyType, valueType, Arrays.asList(additionalClassesToRegister));
	}

	protected <T extends Serializable> BlueCollection<T> initializeCollection(String name, Class<? extends BlueKey> keyType, Class<T> valueType, List<Class<? extends Serializable>> additionalClassesToRegister) throws BlueDbException {
		return initializeCollection(name, keyType, valueType, additionalClassesToRegister, null);
	}

	protected <T extends Serializable> BlueCollection<T> initializeCollection(String name, Class<? extends BlueKey> keyType, Class<T> valueType, List<Class<? extends Serializable>> additionalClassesToRegister, SegmentSizeSetting segmentSize) throws BlueDbException {
		synchronized (collections) {
			@SuppressWarnings("unchecked")
			BlueCollectionOnDisk<T> collection = (BlueCollectionOnDisk<T>) collections.get(name);
			if(collection == null) {
				collection = new BlueCollectionOnDisk<T>(this, name, keyType, valueType, additionalClassesToRegister, segmentSize);
				collections.put(name, collection);
			} else if(!collection.getType().equals(valueType)) {
				throw new BlueDbException("The " + name + " collection already exists for a different type [collectionType=" + collection.getType() + " invalidType=" + valueType + "]");
			} else if (!collection.getKeyType().equals(keyType)) {
				throw new BlueDbException("The " + name + " collection already exists for a different key type (" + collection.getKeyType() + ") vs " + keyType);
			} else {
				assertExistingCollectionIsType(collection, BlueCollection.class);
			}
			
			BlueCollection<T> typedCollection = (BlueCollection<T>) collection;
			return typedCollection;
		}
	}
	
	protected <T extends Serializable> BlueTimeCollection<T> initializeTimeCollection(String name, Class<? extends BlueKey> keyType, Class<T> valueType, List<Class<? extends Serializable>> additionalClassesToRegister, SegmentSizeSetting segmentSize) throws BlueDbException {
		synchronized (collections) {
			@SuppressWarnings("unchecked")
			BlueCollectionOnDisk<T> collection = (BlueCollectionOnDisk<T>) collections.get(name);
			if(collection == null) {
				collection = new BlueTimeCollectionOnDisk<T>(this, name, keyType, valueType, additionalClassesToRegister, segmentSize);
				collections.put(name, collection);
			} else if(!collection.getType().equals(valueType)) {
				throw new BlueDbException("The " + name + " collection already exists for a different type [collectionType=" + collection.getType() + " invalidType=" + valueType + "]");
			} else if (!collection.getKeyType().equals(keyType)) {
				throw new BlueDbException("The " + name + " collection already exists for a different key type (" + collection.getKeyType() + ") vs " + keyType);
			} else {
				assertExistingCollectionIsType(collection, BlueTimeCollection.class);
			}
			
			@SuppressWarnings("unchecked")
			BlueTimeCollection<T> typedCollection = (BlueTimeCollection<T>) collection;
			return typedCollection;
		}
	}
	
	/*
	 * TODO: Regarding getCollection and getTimeCollection: Should this return an existing collection even if the user has not 
	 * initialized/built the collection? I kind of like forcing read/write users to intialize/build the collection first so that
	 * if they make schema changes they always get applied on startup.
	 */

	@Override
	public <T extends Serializable> BlueCollection<T> getCollection(String name, Class<T> valueType) throws BlueDbException {
		synchronized(collections) {
			ReadableBlueCollectionOnDisk<?> untypedCollection = collections.get(name);
			if (untypedCollection == null) {
				return null;
			}
			if (!untypedCollection.getType().equals(valueType)) {
				throw new BlueDbException("Cannot cast BlueCollection<" + untypedCollection.getType() + "> to BlueCollection<" + valueType + ">");
			} else {
				assertExistingCollectionIsType(untypedCollection, BlueCollection.class);
			}
			
			@SuppressWarnings("unchecked")
			BlueCollection<T> typedCollection = (BlueCollection<T>) untypedCollection;
			return typedCollection;
		}
	}

	@Override
	public <V extends Serializable> BlueTimeCollection<V> getTimeCollection(String name, Class<V> valueType) throws BlueDbException {
		BlueCollection<V> collection = getCollection(name, valueType);
		if(collection instanceof BlueTimeCollection) {
			return (BlueTimeCollection<V>) collection;
		} else {
			throw new BlueDbException("Cannot cast " + collection.getClass() + " to " + BlueTimeCollection.class);
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

	protected static void assertExistingCollectionIsType(ReadableBlueCollectionOnDisk<?> collection, Class<?> klazz) throws BlueDbException {
		if(!klazz.isAssignableFrom(collection.getClass())) {
			String name = collection.getPath().toFile().getName();
			throw new BlueDbException("The " + name + " collection already exists but it cannot be cast to a " + klazz.getSimpleName() + ". InvalidType=" + collection.getClass());
		}
	}


	protected List<BlueCollectionOnDisk<?>> getAllCollectionsFromDisk() throws BlueDbException {
		List<File> subfolders = FileUtils.getSubFolders(path.toFile());
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


	public BlueExecutor getSharedExecutor() {
		return sharedExecutor;
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

	public BackupManager getBackupManager() {
		return backupManager;
	}
}
