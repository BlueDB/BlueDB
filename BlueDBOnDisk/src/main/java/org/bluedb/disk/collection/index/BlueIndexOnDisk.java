package org.bluedb.disk.collection.index;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.collection.ReadableBlueCollectionOnDisk;
import org.bluedb.disk.file.FileManager;
import org.bluedb.disk.segment.rollup.Rollupable;

public class BlueIndexOnDisk<I extends ValueKey, T extends Serializable> extends ReadableBlueIndexOnDisk<I, T> implements BlueIndex<I, T>, Rollupable {

	public static <K extends ValueKey, T extends Serializable> BlueIndexOnDisk<K, T> createNew(ReadableBlueCollectionOnDisk<T> collection, Path indexPath, KeyExtractor<K, T> keyExtractor) throws BlueDbException {
		indexPath.toFile().mkdirs();
		FileManager fileManager = collection.getFileManager();
		Path keyExtractorPath = Paths.get(indexPath.toString(), FILE_KEY_EXTRACTOR);
		fileManager.saveObject(keyExtractorPath, keyExtractor);
		BlueIndexOnDisk<K, T> index = new BlueIndexOnDisk<K, T>(collection, indexPath, keyExtractor);
		populateNewIndex(collection, index);
		return index;
	}


	public static <K extends ValueKey, T extends Serializable> BlueIndexOnDisk<K, T> fromExisting(ReadableBlueCollectionOnDisk<T> collection, Path indexPath) throws BlueDbException {
		FileManager fileManager = collection.getFileManager();
		Path keyExtractorPath = Paths.get(indexPath.toString(), FILE_KEY_EXTRACTOR);
		@SuppressWarnings("unchecked")
		KeyExtractor<K, T> keyExtractor = (KeyExtractor<K, T>) fileManager.loadObject(keyExtractorPath);
		return new BlueIndexOnDisk<K, T>(collection, indexPath, keyExtractor);
	}

	private BlueIndexOnDisk(ReadableBlueCollectionOnDisk<T> collection, Path indexPath, KeyExtractor<I, T> keyExtractor) throws BlueDbException {
		super(collection, indexPath, keyExtractor);
	}
}
