package io.bluedb.memory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.nustaq.serialization.FSTConfiguration;

import io.bluedb.api.BlueCollection;
import io.bluedb.api.BlueDb;
import io.bluedb.api.exceptions.BlueDbException;

public class BlueDbInMemory implements BlueDb {
	private static final String COLLECTIONS_FILENAME = "collections.bin";
	private static final String CLASSES_FILENAME = "classes.bin";

	private Path directory;
	
	private Map<String, BlueCollection<? extends Serializable>> collections = new HashMap<>();
	private Map<String, Class<? extends Serializable>> classes = new HashMap<>();
	
	
	public BlueDbInMemory() {
	}
	
	public BlueDbInMemory(Path directory) throws BlueDbException {
		this.directory = directory;
		initializeFromDirectory();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Serializable> BlueCollection<T> getCollection(Class<T> type, String name) throws BlueDbException {
		synchronized(collections) {
			if (!collections.containsKey(name)) {
				collections.put(name, new BlueCollectionImpl<T>(type));
				classes.put(name, type);
			}
			if (classes.get(name) != type) {
				throw new BlueDbException("Collection '" + name + "' is not for type " + type);
			}
			return (BlueCollection<T>)(collections.get(name));
		}
	}

	@Override
	public void backup(Path path) throws BlueDbException {
		serializeToDirectory();
	}

	@Override
	public void shutdown() throws BlueDbException {
		serializeToDirectory();
	}
	
	private void initializeFromDirectory() throws BlueDbException {
		try {
			if(directory != null && Files.isDirectory(directory)) {
				Map<String, BlueCollection<? extends Serializable>> collections = readObjectFromFile(directory.resolve(COLLECTIONS_FILENAME));
				Map<String, Class<? extends Serializable>> classes = readObjectFromFile(directory.resolve(CLASSES_FILENAME));

				if(collections != null && classes != null) {
					this.collections = collections;
					this.classes = classes;
				}
			}
		} catch(Throwable t) {
			throw new BlueDbException("Failed to initalize in memory database from directory " + directory, t);
		}
	}
	
	@SuppressWarnings("unchecked")
	private <T> T readObjectFromFile(Path file) throws IOException {
		FSTConfiguration serializer = FSTConfiguration.getDefaultConfiguration();
		
		if(Files.exists(file) && Files.size(file) > 0) {
			return (T) serializer.asObject(Files.readAllBytes(file));
		}
		
		return null;
	}

	private void serializeToDirectory() throws BlueDbException {
		try {
			if(directory != null) {
				Files.createDirectories(directory);
				
				FSTConfiguration serializer = FSTConfiguration.getDefaultConfiguration();
				
				Files.write(directory.resolve(COLLECTIONS_FILENAME), serializer.asByteArray(collections));
				Files.write(directory.resolve(CLASSES_FILENAME), serializer.asByteArray(classes));
			}
		} catch (Throwable t) {
			throw new BlueDbException("Failed to serialize in memory database to directory " + directory, t);
		}
	}

}
