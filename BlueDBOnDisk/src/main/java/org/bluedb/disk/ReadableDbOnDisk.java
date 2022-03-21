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
import org.bluedb.disk.collection.ReadOnlyCollectionOnDisk;
import org.bluedb.disk.collection.ReadOnlyTimeCollectionOnDisk;
import org.bluedb.disk.config.ConfigurationService;
import org.bluedb.disk.config.ConfigurationServiceWrapper;
import org.bluedb.disk.encryption.EncryptionService;
import org.bluedb.disk.encryption.EncryptionServiceWrapper;
import org.bluedb.disk.time.StandardTimeService;

public class ReadableDbOnDisk implements ReadableBlueDb {

	protected final Path path;
	protected final ConfigurationServiceWrapper configurationService;
	protected final EncryptionServiceWrapper encryptionService;

	private final Map<String, ReadOnlyCollectionOnDisk<? extends Serializable>> collections = new HashMap<>();
	
	ReadableDbOnDisk(Path path, ConfigurationService configurationService, EncryptionService encryptionService) {
		this.path = path;
		this.configurationService = new ConfigurationServiceWrapper(configurationService, new StandardTimeService(), 60_000);
		this.encryptionService = new EncryptionServiceWrapper(encryptionService);
	}
	
	@Override
	public <T extends Serializable> ReadableBlueCollection<T> getCollection(String name, Class<T> valueType) throws BlueDbException {
		try {
			return getExistingCollection(name, valueType);
		} catch (NoSuchCollectionException e) {
			return new FacadeCollection<>(this, name, valueType);
		}
	}

	private <T extends Serializable> ReadOnlyCollectionOnDisk<?> getUntypedCollectionIfExists(String name, Class<T> valueType) throws BlueDbException {
		synchronized(collections) {
			ReadOnlyCollectionOnDisk<?> collection = collections.get(name);
			if (collection != null) {
				return collection;
			} else if (collectionFolderExists(name)) {
				ReadOnlyCollectionOnDisk<T> newCollection = instantiateCollectionFromExistingOnDisk(name, valueType);
				collections.put(name, newCollection);
				return newCollection;
			} else {
				return null;
			}
		}
	}

	public <T extends Serializable> ReadableBlueCollection<T> getExistingCollection(String name, Class<T> valueType) throws BlueDbException {
		ReadOnlyCollectionOnDisk<?> untypedCollection = getUntypedCollectionIfExists(name, valueType);
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

	private <T extends Serializable> ReadOnlyCollectionOnDisk<T> instantiateCollectionFromExistingOnDisk(String name, Class<T> valueType) throws BlueDbException {
		ReadOnlyCollectionOnDisk<T> collection = new ReadOnlyCollectionOnDisk<>(this, name, null, valueType, Arrays.asList());
		if (TimeKey.class.isAssignableFrom(collection.getKeyType())) {
			Class<? extends BlueKey> keyType = collection.getKeyType();
			collection = new ReadOnlyTimeCollectionOnDisk<>(this, name, keyType, valueType, Arrays.asList());
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
	
	public ConfigurationServiceWrapper getConfigurationService() { return configurationService; }

	public EncryptionServiceWrapper getEncryptionService() { return encryptionService; }

}
