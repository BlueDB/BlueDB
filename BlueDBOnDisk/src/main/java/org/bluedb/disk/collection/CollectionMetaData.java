package org.bluedb.disk.collection;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.file.FileManager;
import org.bluedb.disk.segment.SegmentSizeSetting;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;

public class CollectionMetaData {
	
	private static final String FILENAME_SERIALIZED_CLASSES = "serialized_classes";
	private static final String FILENAME_KEY_TYPE = "key_type";
	private static final String FILENAME_SEGMENT_SIZE = "segment_size";
	private static final String META_DATA_FOLDER = ".meta";
	
	final Path folderPath;
	final FileManager fileManager;
	final Path serializedClassesPath;
	final Path keyTypePath;
	final Path segmentSizePath;

	public CollectionMetaData(Path collectionPath) {
		// meta data needs its own serialized because collection doesn't know which classes to register until metadata deserializes them from disk
		BlueSerializer serializer = new ThreadLocalFstSerializer();
		
		fileManager = new FileManager(serializer);  
		folderPath = Paths.get(collectionPath.toString(), META_DATA_FOLDER);
		serializedClassesPath = Paths.get(folderPath.toString(), FILENAME_SERIALIZED_CLASSES);
		keyTypePath = Paths.get(folderPath.toString(), FILENAME_KEY_TYPE);
		segmentSizePath = Paths.get(folderPath.toString(), FILENAME_SEGMENT_SIZE);
	}

	@SuppressWarnings("unchecked")
	public Class<? extends BlueKey> getKeyType() throws BlueDbException {
		Object savedValue = fileManager.loadObject(keyTypePath);
		return (Class<? extends BlueKey>) savedValue;
	}

	public SegmentSizeSetting getSegmentSize() throws BlueDbException {
		Object savedValue = fileManager.loadObject(segmentSizePath);
		return (SegmentSizeSetting) savedValue;
	}

	public void saveSegmentSize(SegmentSizeSetting segmentSize) throws BlueDbException {
		fileManager.saveObject(segmentSizePath, segmentSize);
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
		fileManager.saveObject(serializedClassesPath, classes);
	}

	public void saveKeyType(Class<? extends BlueKey> keyType) throws BlueDbException {
		fileManager.saveObject(keyTypePath, keyType);
	}

	public Path getPath() {
		return folderPath;
	}

	public final Class<? extends Serializable>[] getAndAddToSerializedClassList(Class<? extends Serializable> primaryClass, List<Class<? extends Serializable>> additionalClasses) throws BlueDbException {
		List<Class<? extends Serializable>> existingClassList = getSerializedClassList();
		List<Class<? extends Serializable>> newClassList = (existingClassList == null) ? new ArrayList<>() : new ArrayList<>(existingClassList);
		
		boolean madeAChange = false;
		
		for (Class<? extends Serializable> clazz: ThreadLocalFstSerializer.getClassesToAlwaysRegister()) {
			if (!newClassList.contains(clazz)) {  // don't keep expanding the list every time we call this method
				newClassList.add(clazz);
				madeAChange = true;
			}
		}
		
		if (!newClassList.contains(primaryClass)) {  // don't keep expanding the list every time we call this method
			newClassList.add(primaryClass);
			madeAChange = true;
		}
		
		for (Class<? extends Serializable> clazz: additionalClasses) {
			if (!newClassList.contains(clazz)) {  // don't keep expanding the list every time we call this method
				newClassList.add(clazz);
				madeAChange = true;
			}
		}
		
		if(madeAChange) {
			updateSerializedClassList(newClassList);
		}
		
		@SuppressWarnings("unchecked")
		Class<? extends Serializable>[] returnValue = newClassList.toArray(new Class[newClassList.size()]);
		return returnValue;
	}
}