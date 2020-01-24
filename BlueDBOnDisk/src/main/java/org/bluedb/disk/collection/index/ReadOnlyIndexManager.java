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
import org.bluedb.disk.collection.ReadOnlyCollectionOnDisk;
import org.bluedb.disk.file.FileUtils;

public class ReadOnlyIndexManager<T extends Serializable> extends ReadableIndexManager<T> {

	private final Map<String, ReadOnlyIndexOnDisk<ValueKey, T>> indexesByName;
	private final ReadOnlyCollectionOnDisk<T> collection;

	public ReadOnlyIndexManager(ReadOnlyCollectionOnDisk<T> collection, Path collectionPath) throws BlueDbException {
		this.collection = collection;
		this.indexesByName = getIndexesFromDisk(collection, collectionPath);
	}

	public <K extends ValueKey> ReadOnlyIndexOnDisk<K, T> getIndex(String indexName, Class<K> keyType) throws BlueDbException {
		ReadOnlyIndexOnDisk<ValueKey, T> index = indexesByName.get(indexName);
		if (index == null) {
			index = getIndexFromDisk(indexName);
		}
		if (index.getType() != keyType) {
			throw new BlueDbException("Invalid type (" + keyType.getName() + ") for index " + indexName + " of type " + index.getType());
		}
		@SuppressWarnings("unchecked")
		ReadOnlyIndexOnDisk<K, T> typedIndex = (ReadOnlyIndexOnDisk<K, T>) index;
		return typedIndex;
	}

	private ReadOnlyIndexOnDisk<ValueKey, T> getIndexFromDisk(String indexName) throws BlueDbException {
		Path indexPath = Paths.get(collection.getPath().toString(), ".index", indexName);
		if(indexPath.toFile().exists()) {
			ReadOnlyIndexOnDisk<ValueKey, T> index = ReadOnlyIndexOnDisk.fromExisting(collection, indexPath);
			indexesByName.put(indexName, index);
		} else {
			throw new NoSuchIndexException("No such index: " + indexName);
		}
		return indexesByName.get(indexName);
	}

	private Map<String, ReadOnlyIndexOnDisk<ValueKey, T>> getIndexesFromDisk(ReadOnlyCollectionOnDisk<T> collection, Path collectionPath) throws BlueDbException {
		Map<String, ReadOnlyIndexOnDisk<ValueKey, T>> map = new HashMap<>();
		Path indexesPath = Paths.get(collectionPath.toString(), INDEXES_SUBFOLDER);
		List<File> subfolders = FileUtils.getSubFolders(indexesPath.toFile());
		for (File folder: subfolders) {
			ReadOnlyIndexOnDisk<ValueKey, T> index = ReadOnlyIndexOnDisk.fromExisting(collection, folder.toPath());
			String indexName = folder.getName();
			map.put(indexName, index);
		}
		return map;
	}
}
