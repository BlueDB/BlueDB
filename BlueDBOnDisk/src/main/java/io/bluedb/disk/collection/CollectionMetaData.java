package io.bluedb.disk.collection;

import java.nio.file.Path;
import java.nio.file.Paths;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.file.FileManager;

public class CollectionMetaData {
	
	private static final String FILENAME_MAX_INTEGER = "max_int";
	private static final String FILENAME_MAX_LONG = "max_long";
	private static final String META_DATA_FOLDER = ".meta";
	
	final Path folderPath;
	final FileManager fileManager;
	final Path maxIntegerPath;
	final Path maxLongPath;

	public CollectionMetaData(Path collectionPath, FileManager fileManager) {
		folderPath = Paths.get(collectionPath.toString(), META_DATA_FOLDER);
		maxIntegerPath = Paths.get(folderPath.toString(), FILENAME_MAX_INTEGER);
		maxLongPath = Paths.get(folderPath.toString(), FILENAME_MAX_LONG);
		this.fileManager = fileManager;
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

	public Path getPath() {
		return folderPath;
	}
}