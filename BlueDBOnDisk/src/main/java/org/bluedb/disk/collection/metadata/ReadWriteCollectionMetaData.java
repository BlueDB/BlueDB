package org.bluedb.disk.collection.metadata;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.encryption.EncryptionServiceWrapper;
import org.bluedb.disk.file.ReadWriteFileManager;
import org.bluedb.disk.segment.SegmentSizeSetting;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;

public class ReadWriteCollectionMetaData extends ReadableCollectionMetadata {
	

	final ReadWriteFileManager fileManager;

	public ReadWriteCollectionMetaData(Path collectionPath, EncryptionServiceWrapper encryptionService) {
		super(collectionPath);
		// meta data needs its own serializer because collection doesn't know which classes to register until metadata deserializes them from disk
		BlueSerializer serializer = new ThreadLocalFstSerializer();
		fileManager = new ReadWriteFileManager(serializer, encryptionService);  
	}

	@Override
	public ReadWriteFileManager getFileManager() {
		return fileManager;
	}

	public void saveSegmentSize(SegmentSizeSetting segmentSize) throws BlueDbException {
		getFileManager().saveObject(segmentSizePath, segmentSize);
	}

	public void updateSerializedClassList(List<Class<? extends Serializable>> classes) throws BlueDbException {
		getFileManager().saveVersionedObject(folderPath, FILENAME_SERIALIZED_CLASSES, classes);
	}

	public void saveKeyType(Class<? extends BlueKey> keyType) throws BlueDbException {
		getFileManager().saveObject(keyTypePath, keyType);
	}

	public final Class<? extends Serializable>[] getAndAddToSerializedClassList(Class<? extends Serializable> primaryClass, List<Class<? extends Serializable>> additionalClasses) throws BlueDbException {
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
		
		if(!Objects.equals(existingClassList, newClassList)) {
			updateSerializedClassList(newClassList);
		}
		
		@SuppressWarnings("unchecked")
		Class<? extends Serializable>[] returnValue = newClassList.toArray(new Class[newClassList.size()]);
		return returnValue;
	}
}