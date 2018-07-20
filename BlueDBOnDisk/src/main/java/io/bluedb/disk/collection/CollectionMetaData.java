package io.bluedb.disk.collection;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.file.FileManager;
import io.bluedb.disk.serialization.BlueSerializer;
import io.bluedb.disk.serialization.ThreadLocalFstSerializer;

public class CollectionMetaData {
	
	private static final String FILENAME_MAX_INTEGER = "max_int";
	private static final String FILENAME_MAX_LONG = "max_long";
	private static final String FILENAME_SERIALIZED_CLASSES = "serialized_classes";
	private static final String META_DATA_FOLDER = ".meta";
	
	final Path folderPath;
	final FileManager fileManager;
	final Path maxIntegerPath;
	final Path maxLongPath;
	final Path serializedClassesPath;

	public CollectionMetaData(Path collectionPath) {
		// meta data needs its own serialized because collection doesn't know which classes to register until metadata deserializes them from disk
		BlueSerializer serializer = new ThreadLocalFstSerializer();
		
		fileManager = new FileManager(serializer);  
		folderPath = Paths.get(collectionPath.toString(), META_DATA_FOLDER);
		maxIntegerPath = Paths.get(folderPath.toString(), FILENAME_MAX_INTEGER);
		maxLongPath = Paths.get(folderPath.toString(), FILENAME_MAX_LONG);
		serializedClassesPath = Paths.get(folderPath.toString(), FILENAME_SERIALIZED_CLASSES);
	}

	public List<Class<? extends Serializable>> getSerializedClassList() throws BlueDbException {
		Object savedValue = fileManager.loadObject(serializedClassesPath);
		try {
			@SuppressWarnings("unchecked")
			List<Class<? extends Serializable>> typedList = (List<Class<? extends Serializable>>) savedValue;
			return typedList;
		} catch(ClassCastException e) {
			e.printStackTrace();
			throw new BlueDbException("Serialized class list in collection data is corrupted: " + serializedClassesPath, e);
		}
	}

	public Long getMaxLong() throws BlueDbException {
		return (Long) fileManager.loadObject(maxLongPath);
	}

	public Integer getMaxInteger() throws BlueDbException {
		return (Integer) fileManager.loadObject(maxIntegerPath);
	}


	public void updateMaxLong(long value) throws BlueDbException {
		Long currentMaxLong = getMaxLong();
		if (currentMaxLong == null || currentMaxLong < value) {
			fileManager.saveObject(maxLongPath, value);;
		}
	}

	public void updateMaxInteger(int value) throws BlueDbException {
		Integer currentMaxInteger = getMaxInteger();
		if (currentMaxInteger == null || currentMaxInteger < value) {
			fileManager.saveObject(maxIntegerPath, value);;
		}
	}

	public void updateSerializedClassList(List<Class<? extends Serializable>> classes) throws BlueDbException {
		fileManager.saveObject(serializedClassesPath, classes);;
	}

	public Path getPath() {
		return folderPath;
	}

	@SafeVarargs
	public final Class<? extends Serializable>[] getAndAddToSerializedClassList(Class<? extends Serializable>... additionalClasses) throws BlueDbException {
		List<Class<? extends Serializable>> existingClassList = getSerializedClassList();
		List<Class<? extends Serializable>> newClassList = (existingClassList == null) ? new ArrayList<>() : new ArrayList<>(existingClassList);
		for (Class<? extends Serializable> clazz: additionalClasses) {
			if (!newClassList.contains(clazz)) {  // don't keep expanding the list every time we call this method
				newClassList.add(clazz);
			}
		}
		updateSerializedClassList(newClassList);
		@SuppressWarnings("unchecked")
		Class<? extends Serializable>[] returnValue = newClassList.toArray(new Class[newClassList.size()]);
		return returnValue;
	}
}