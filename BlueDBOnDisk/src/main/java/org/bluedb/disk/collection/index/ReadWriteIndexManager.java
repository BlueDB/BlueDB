package org.bluedb.disk.collection.index;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.file.FileUtils;
import org.bluedb.disk.recovery.IndividualChange;

public class ReadWriteIndexManager<T extends Serializable> extends ReadableIndexManager<T> {

	private final ReadWriteCollectionOnDisk<T> collection;
	private Map<String, ReadWriteIndexOnDisk<ValueKey, T>> indexesByName;

	public ReadWriteIndexManager(ReadWriteCollectionOnDisk<T> collection, Path collectionPath) throws BlueDbException {
		this.collection = collection;
		indexesByName = getIndexesFromDisk(collection, collectionPath);
	}

	public <K extends ValueKey> BlueIndex<K, T> getOrCreate(String indexName, Class<K> keyType, KeyExtractor<K, T> keyExtractor) throws BlueDbException {
		if (indexesByName.containsKey(indexName)) {
			return getIndex(indexName, keyType);
		}
		Path indexPath = Paths.get(collection.getPath().toString(), INDEXES_SUBFOLDER, indexName);
		ReadWriteIndexOnDisk<K, T> index = ReadWriteIndexOnDisk.createNew(collection, indexPath, keyExtractor);
		@SuppressWarnings("unchecked")
		ReadWriteIndexOnDisk<ValueKey, T> typedIndex = (ReadWriteIndexOnDisk<ValueKey, T>) index;
		indexesByName.put(indexName, typedIndex);
		return index;
	}

	public ReadWriteIndexOnDisk<?, T> getUntypedIndex(String indexName) throws BlueDbException {
		return indexesByName.get(indexName);
	}
	
	public <K extends ValueKey> ReadWriteIndexOnDisk<K, T> getIndex(String indexName, Class<K> keyType) throws BlueDbException {
		ReadableIndexOnDisk<ValueKey, T> index = indexesByName.get(indexName);
		if (index == null) {
			throw new BlueDbException("No such index: " + indexName);
		} else if (index.getType() != keyType) {
			throw new BlueDbException("Invalid type (" + keyType.getName() + ") for index " + indexName + " of type " + index.getType());
		}
		@SuppressWarnings("unchecked")
		ReadWriteIndexOnDisk<K, T> typedIndex = (ReadWriteIndexOnDisk<K, T>) index;
		return typedIndex;
	}

	public void indexChange(BlueKey key, T oldValue, T newValue) throws BlueDbException {
		for (ReadWriteIndexOnDisk<ValueKey, T> index: indexesByName.values()) {
			index.indexChange(key, oldValue, newValue);
		}
	}

	public void indexChanges(Collection<IndividualChange<T>> changes) throws BlueDbException {
		for (ReadWriteIndexOnDisk<ValueKey, T> index: indexesByName.values()) {
			index.indexChanges(changes);
		}
	}

	private Map<String, ReadWriteIndexOnDisk<ValueKey, T>> getIndexesFromDisk(ReadWriteCollectionOnDisk<T> collection, Path collectionPath) throws BlueDbException {
		Map<String, ReadWriteIndexOnDisk<ValueKey, T>> map = new HashMap<>();
		Path indexesPath = Paths.get(collectionPath.toString(), INDEXES_SUBFOLDER);
		List<File> subfolders = FileUtils.getSubFolders(indexesPath.toFile());
		for (File folder: subfolders) {
			ReadWriteIndexOnDisk<ValueKey, T> index = ReadWriteIndexOnDisk.fromExisting(collection, folder.toPath());
			String indexName = folder.getName();
			map.put(indexName, index);
		}
		return map;
	}
}
