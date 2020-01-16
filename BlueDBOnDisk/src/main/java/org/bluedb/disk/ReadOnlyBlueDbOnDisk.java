package org.bluedb.disk;

import java.io.Serializable;
import java.nio.file.Path;
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
import org.bluedb.disk.collection.ReadOnlyBlueCollectionOnDisk;
import org.bluedb.disk.collection.ReadOnlyBlueTimeCollectionOnDisk;

public class ReadOnlyBlueDbOnDisk implements ReadableBlueDb {

	protected final Path path;

	private final Map<String, ReadOnlyBlueCollectionOnDisk<? extends Serializable>> collections = new HashMap<>();
	
	ReadOnlyBlueDbOnDisk(Path path) {
		this.path = path;
	}
	
	//TODO: getCollection and getTimeCollection need to work even if they haven't called initialize or build. Return empty collection object if it doesn't exist
	//TODO: Should getTimeCollection throw an exception if the key ove the collection isn't a TimeKey?

	@Override
	public <T extends Serializable> ReadableBlueCollection<T> getCollection(String name, Class<T> valueType) throws BlueDbException {
		synchronized(collections) {
			ReadOnlyBlueCollectionOnDisk<?> untypedCollection = collections.get(name);
			if (untypedCollection == null) {
				ReadOnlyBlueCollectionOnDisk<T> collection = new ReadOnlyBlueCollectionOnDisk<>(this, name, null, valueType, Arrays.asList());
				if (TimeKey.class.isAssignableFrom(collection.getKeyType())) {
					Class<? extends BlueKey> keyType = collection.getKeyType();
					collection = new ReadOnlyBlueTimeCollectionOnDisk<>(this, name, keyType, valueType, Arrays.asList());
				}
				collections.put(name, collection);
				return collection;
			}
			if (untypedCollection.getType().equals(valueType)) {
				@SuppressWarnings("unchecked")
				ReadableBlueCollection<T> typedCollection = (ReadableBlueCollection<T>) untypedCollection;
				return typedCollection;
			} else {
				throw new BlueDbException("Cannot cast BlueCollection<" + untypedCollection.getType() + "> to BlueCollection<" + valueType + ">");
			}
		}
	}

	@Override
	public <V extends Serializable> ReadableBlueTimeCollection<V> getTimeCollection(String name, Class<V> valueType) throws BlueDbException {
		ReadableBlueCollection<V> collection = getCollection(name, valueType);
		if(collection instanceof ReadableBlueTimeCollection) {
			return (ReadableBlueTimeCollection<V>) collection;
		} else {
			throw new BlueDbException("Cannot cast " + collection.getClass() + " to " + ReadableBlueTimeCollection.class);
		}
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
