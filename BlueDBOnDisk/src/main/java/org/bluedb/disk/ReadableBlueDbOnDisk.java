package org.bluedb.disk;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.bluedb.api.ReadableBlueCollection;
import org.bluedb.api.ReadableBlueDb;
import org.bluedb.api.ReadableBlueTimeCollection;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.collection.FacadeCollection;
import org.bluedb.disk.collection.FacadeTimeCollection;
import org.bluedb.disk.collection.NoSuchCollectionException;
import org.bluedb.disk.collection.ReadOnlyBlueCollectionOnDisk;
import org.bluedb.disk.collection.ReadOnlyBlueTimeCollectionOnDisk;

public class ReadableBlueDbOnDisk implements ReadableBlueDb {

	protected final Path path;

	private final Map<String, ReadOnlyBlueCollectionOnDisk<? extends Serializable>> collections = new HashMap<>();
	
	ReadableBlueDbOnDisk(Path path) {
		this.path = path;
	}
	
	@Override
	public <T extends Serializable> ReadableBlueCollection<T> getCollection(String name, Class<T> valueType) throws BlueDbException {
		try {
			return getExistingCollection(name, valueType);
		} catch (NoSuchCollectionException e) {
			return new FacadeCollection<>(this, name, valueType);
		}
	}

	private <T extends Serializable> ReadOnlyBlueCollectionOnDisk<?> getUntypedCollectionIfExists(String name, Class<T> valueType) throws BlueDbException {
		synchronized(collections) {
			ReadOnlyBlueCollectionOnDisk<?> collection = collections.get(name);
			if (collection != null) {
				return collection;
			} else if (collectionFolderExists(name)) {
				ReadOnlyBlueCollectionOnDisk<T> newCollection = instantiateCollectionFromExistingOnDisk(name, valueType);
				collections.put(name, newCollection);
				return newCollection;
			} else {
				return null;
			}
		}
	}

	public <T extends Serializable> ReadableBlueCollection<T> getExistingCollection(String name, Class<T> valueType) throws BlueDbException {
		ReadOnlyBlueCollectionOnDisk<?> untypedCollection = getUntypedCollectionIfExists(name, valueType);
		if (untypedCollection == null) {
			throw new NoSuchCollectionException("no such collection: " + name);
		} else if (untypedCollection.getType().equals(valueType)) {
			@SuppressWarnings("unchecked")
			ReadableBlueCollection<T> typedCollection = (ReadableBlueCollection<T>) untypedCollection;
			return typedCollection;
		} else {
			throw new BlueDbException("Cannot cast BlueCollection<" + untypedCollection.getType() + "> to BlueCollection<" + valueType + ">");
		}
	}

	private <T extends Serializable> ReadOnlyBlueCollectionOnDisk<T> instantiateCollectionFromExistingOnDisk(String name, Class<T> valueType) throws BlueDbException {
		ReadOnlyBlueCollectionOnDisk<T> collection = new ReadOnlyBlueCollectionOnDisk<>(this, name, null, valueType, Arrays.asList());
		if (TimeKey.class.isAssignableFrom(collection.getKeyType())) {
			Class<? extends BlueKey> keyType = collection.getKeyType();
			collection = new ReadOnlyBlueTimeCollectionOnDisk<>(this, name, keyType, valueType, Arrays.asList());
		}
		return collection;
	}

	@Override
	public <V extends Serializable> ReadableBlueTimeCollection<V> getTimeCollection(String name, Class<V> valueType) throws BlueDbException {
		try {
			ReadableBlueCollection<V> collection = getExistingCollection(name, valueType);
			if(collection instanceof ReadableBlueTimeCollection) {
				return (ReadableBlueTimeCollection<V>) collection;
			} else {
				throw new BlueDbException("Cannot cast " + collection.getClass() + " to " + ReadableBlueTimeCollection.class);
			}
		} catch (NoSuchCollectionException e) {
			return new FacadeTimeCollection<V>(this, name, valueType);
		}
	}

	public boolean collectionFolderExists(String name) {
		return Paths.get(path.toString(), name).toFile().exists();
	}

	@Override
	public void shutdown() {
	}
	
	@Override
	public void shutdownNow() {
	}
	
	@Override
	public boolean awaitTermination(long timeout, TimeUnit timeUnit) throws BlueDbException {
		return true;
	}

	public Path getPath() {
		return path;
	}
}
