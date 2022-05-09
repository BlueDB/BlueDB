package org.bluedb.disk.collection.metadata;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.bluedb.api.BlueCollectionVersion;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.file.ReadFileManager;
import org.bluedb.disk.segment.SegmentSizeSetting;

public abstract class ReadableCollectionMetadata {
	
	public static final String FILENAME_SERIALIZED_CLASSES = "serialized_classes";
	private static final String FILENAME_KEY_TYPE = "key_type";
	private static final String FILENAME_SEGMENT_SIZE = "segment_size";
	private static final String FILENAME_COLLECTION_VERSION = "collection_version";
	private static final String META_DATA_FOLDER = ".meta";
	
	final Path folderPath;
	final Path keyTypePath;
	final Path segmentSizePath;
	final Path collectionVersionPath;

	public abstract ReadFileManager getFileManager();

	public ReadableCollectionMetadata(Path collectionPath) {
		// meta data needs its own serialized because collection doesn't know which classes to register until metadata deserializes them from disk
		folderPath = Paths.get(collectionPath.toString(), META_DATA_FOLDER);
		keyTypePath = Paths.get(folderPath.toString(), FILENAME_KEY_TYPE);
		segmentSizePath = Paths.get(folderPath.toString(), FILENAME_SEGMENT_SIZE);
		collectionVersionPath = Paths.get(folderPath.toString(), FILENAME_COLLECTION_VERSION);
	}

	@SuppressWarnings("unchecked")
	public Class<? extends BlueKey> getKeyType() throws BlueDbException {
		Object savedValue = getFileManager().loadObject(keyTypePath);
		return (Class<? extends BlueKey>) savedValue;
	}

	public SegmentSizeSetting getSegmentSize() throws BlueDbException {
		Object savedValue = getFileManager().loadObject(segmentSizePath);
		return (SegmentSizeSetting) savedValue;
	}

	public BlueCollectionVersion getCollectionVersion() throws BlueDbException {
		Object savedValue = getFileManager().loadObject(collectionVersionPath);
		return (BlueCollectionVersion) savedValue;
	}

	public List<Class<? extends Serializable>> getSerializedClassList() throws BlueDbException {
		try {
			Object savedValue = getFileManager().loadVersionedObject(folderPath, FILENAME_SERIALIZED_CLASSES);
			@SuppressWarnings("unchecked")
			List<Class<? extends Serializable>> typedList = (List<Class<? extends Serializable>>) savedValue;
			return typedList;
		} catch(ClassCastException | IOException e) {
			e.printStackTrace();
			throw new BlueDbException("Serialized class list in collection data is corrupted: " + Paths.get(folderPath.toString(), FILENAME_SERIALIZED_CLASSES), e);
		}
	}

	public Path getPath() {
		return folderPath;
	}
}