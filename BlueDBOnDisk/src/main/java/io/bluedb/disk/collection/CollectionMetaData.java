package io.bluedb.disk.collection;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.file.FileManager;
import io.bluedb.disk.serialization.BlueSerializer;
import io.bluedb.disk.serialization.ThreadLocalFstSerializer;

public class CollectionMetaData {
	
	private static final String FILENAME_SERIALIZED_CLASSES = "serialized_classes";
	private static final String FILENAME_KEY_TYPE = "key_type";
	private static final String META_DATA_FOLDER = ".meta";
	
	final Path folderPath;
	final FileManager fileManager;
	final Path serializedClassesPath;
	final Path keyTypePath;

	public CollectionMetaData(Path collectionPath) {
		// meta data needs its own serialized because collection doesn't know which classes to register until metadata deserializes them from disk
		BlueSerializer serializer = new ThreadLocalFstSerializer();
		
		fileManager = new FileManager(serializer);  
		folderPath = Paths.get(collectionPath.toString(), META_DATA_FOLDER);
		serializedClassesPath = Paths.get(folderPath.toString(), FILENAME_SERIALIZED_CLASSES);
		keyTypePath = Paths.get(folderPath.toString(), FILENAME_KEY_TYPE);
	}

	@SuppressWarnings("unchecked")
	public Class<? extends BlueKey> getKeyType() throws BlueDbException {
		Object savedValue = fileManager.loadObject(keyTypePath);
		return (Class<? extends BlueKey>) savedValue;
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

	public void updateSerializedClassList(List<Class<? extends Serializable>> classes) throws BlueDbException {
		fileManager.saveObject(serializedClassesPath, classes);;
	}

	public void saveKeyType(Class<? extends BlueKey> keyType) throws BlueDbException {
		fileManager.saveObject(keyTypePath, keyType);
	}

	public Path getPath() {
		return folderPath;
	}

	@SafeVarargs
	public final Class<? extends Serializable>[] getAndAddToSerializedClassList(Class<? extends Serializable> primaryClass, Class<? extends Serializable>... additionalClasses) throws BlueDbException {
		List<Class<? extends Serializable>> existingClassList = getSerializedClassList();
		List<Class<? extends Serializable>> newClassList = (existingClassList == null) ? new ArrayList<>() : new ArrayList<>(existingClassList);
		for (Class<? extends Serializable> clazz: ThreadLocalFstSerializer.getClassesToAlwaysRegister()) {
			if (!newClassList.contains(clazz)) {  // don't keep expanding the list every time we call this method
				newClassList.add(clazz);
			}
		}
		if (!newClassList.contains(primaryClass)) {  // don't keep expanding the list every time we call this method
			newClassList.add(primaryClass);
		}
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