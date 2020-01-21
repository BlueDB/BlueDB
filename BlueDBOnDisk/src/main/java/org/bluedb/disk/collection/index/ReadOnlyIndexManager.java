package org.bluedb.disk.collection.index;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.collection.ReadOnlyBlueCollectionOnDisk;
import org.bluedb.disk.file.FileUtils;

public class ReadOnlyIndexManager<T extends Serializable> extends ReadableIndexManager<T> {

	private final Map<String, ReadOnlyBlueIndexOnDisk<ValueKey, T>> indexesByName;
	private final ReadOnlyBlueCollectionOnDisk<T> collection;

	public ReadOnlyIndexManager(ReadOnlyBlueCollectionOnDisk<T> collection, Path collectionPath) throws BlueDbException {
		this.collection = collection;
		this.indexesByName = getIndexesFromDisk(collection, collectionPath);
	}

	public <K extends ValueKey> ReadOnlyBlueIndexOnDisk<K, T> getIndex(String indexName, Class<K> keyType) throws BlueDbException {
		ReadOnlyBlueIndexOnDisk<ValueKey, T> index = indexesByName.get(indexName);
		if (index == null) {
			index = getIndexFromDisk(indexName);
		}
		if (index == null) {
			throw new NoSuchIndexException("No such index: " + indexName);
		} else if (index.getType() != keyType) {
			throw new BlueDbException("Invalid type (" + keyType.getName() + ") for index " + indexName + " of type " + index.getType());
		}
		@SuppressWarnings("unchecked")
		ReadOnlyBlueIndexOnDisk<K, T> typedIndex = (ReadOnlyBlueIndexOnDisk<K, T>) index;
		return typedIndex;
	}

	private ReadOnlyBlueIndexOnDisk<ValueKey, T> getIndexFromDisk(String indexName) {
		Path indexPath = Paths.get(collection.getPath().toString(), ".index", indexName);
		if (indexPath.toFile().exists()) {
			try {
				ReadOnlyBlueIndexOnDisk<ValueKey, T> index = ReadOnlyBlueIndexOnDisk.fromExisting(collection, indexPath);
				indexesByName.put(indexName, index);
			} catch (BlueDbException e) {}
		}
		return indexesByName.get(indexName);
	}

	private Map<String, ReadOnlyBlueIndexOnDisk<ValueKey, T>> getIndexesFromDisk(ReadOnlyBlueCollectionOnDisk<T> collection, Path collectionPath) throws BlueDbException {
		Map<String, ReadOnlyBlueIndexOnDisk<ValueKey, T>> map = new HashMap<>();
		Path indexesPath = Paths.get(collectionPath.toString(), INDEXES_SUBFOLDER);
		List<File> subfolders = FileUtils.getSubFolders(indexesPath.toFile());
		for (File folder: subfolders) {
			ReadOnlyBlueIndexOnDisk<ValueKey, T> index = ReadOnlyBlueIndexOnDisk.fromExisting(collection, folder.toPath());
			String indexName = folder.getName();
			map.put(indexName, index);
		}
		return map;
	}
}
