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

	private Map<String, ReadOnlyBlueIndexOnDisk<ValueKey, T>> indexesByName;

	public ReadOnlyIndexManager(ReadOnlyBlueCollectionOnDisk<T> collection, Path collectionPath) throws BlueDbException {
		indexesByName = getIndexesFromDisk(collection, collectionPath);
	}

	public ReadOnlyBlueIndexOnDisk<?, T> getUntypedIndex(String indexName) throws BlueDbException {
		return indexesByName.get(indexName);
	}

	public <K extends ValueKey> ReadOnlyBlueIndexOnDisk<K, T> getIndex(String indexName, Class<K> keyType) throws BlueDbException {
		ReadOnlyBlueIndexOnDisk<ValueKey, T> index = indexesByName.get(indexName);
		if (index.getType() != keyType) {
			throw new BlueDbException("Invalid type (" + keyType.getName() + ") for index " + indexName + " of type " + index.getType());
		}
		@SuppressWarnings("unchecked")
		ReadOnlyBlueIndexOnDisk<K, T> typedIndex = (ReadOnlyBlueIndexOnDisk<K, T>) index;
		return typedIndex;
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
