package io.bluedb.disk.collection.index;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.index.BlueIndex;
import io.bluedb.api.index.KeyExtractor;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.ValueKey;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.file.FileUtils;
import io.bluedb.disk.recovery.IndividualChange;

public class IndexManager<T extends Serializable> {

	private static final String INDEXES_SUBFOLDER = ".index";

	private final BlueCollectionOnDisk<T> collection;
	private Map<String, BlueIndexOnDisk<ValueKey, T>> indexesByName;

	public IndexManager(BlueCollectionOnDisk<T> collection, Path collectionPath) throws BlueDbException {
		this.collection = collection;
		indexesByName = getIndexesFromDisk(collection, collectionPath);
	}

	public <K extends ValueKey> BlueIndex<K, T> createIndex(String indexName, Class<K> keyType, KeyExtractor<K, T> keyExtractor) throws BlueDbException {
		Path indexPath = Paths.get(collection.getPath().toString(), INDEXES_SUBFOLDER, indexName);
		BlueIndexOnDisk<K, T> index = BlueIndexOnDisk.createNew(collection, indexPath, keyExtractor);
		@SuppressWarnings("unchecked")
		BlueIndexOnDisk<ValueKey, T> typedIndex = (BlueIndexOnDisk<ValueKey, T>) index;
		indexesByName.put(indexName, typedIndex);
		return index;
	}

	public BlueIndexOnDisk<?, T> getUntypedIndex(String indexName) throws BlueDbException {
		return indexesByName.get(indexName);
	}
	
	public <K extends ValueKey> BlueIndex<K, T> getIndex(String indexName, Class<K> keyType) throws BlueDbException {
		BlueIndexOnDisk<ValueKey, T> index = indexesByName.get(indexName);
		if (index.getType() != keyType) {
			throw new BlueDbException("Invalid type (" + keyType.getName() + ") for index " + indexName + " of type " + index.getType());
		}
		@SuppressWarnings("unchecked")
		BlueIndex<K, T> typedIndex = (BlueIndex<K, T>) index;
		return typedIndex;
	}

	public void removeFromAllIndexes(BlueKey key, T value) throws BlueDbException {
		for (BlueIndexOnDisk<ValueKey, T> index: indexesByName.values()) {
			index.remove(key, value);
		}
	}

	public void addToAllIndexes(BlueKey key, T value) throws BlueDbException {
		for (BlueIndexOnDisk<ValueKey, T> index: indexesByName.values()) {
			index.add(key,  value);
		}
	}

	public void addToAllIndexes(Collection<IndividualChange<T>> changes) throws BlueDbException {
		for (BlueIndexOnDisk<ValueKey, T> index: indexesByName.values()) {
			index.add(changes);
		}
	}

	private Map<String, BlueIndexOnDisk<ValueKey, T>> getIndexesFromDisk(BlueCollectionOnDisk<T> collection, Path collectionPath) throws BlueDbException {
		Map<String, BlueIndexOnDisk<ValueKey, T>> map = new HashMap<>();
		Path indexesPath = Paths.get(collectionPath.toString(), INDEXES_SUBFOLDER);
		List<File> subfolders = FileUtils.getFolderContents(indexesPath.toFile(), (f) -> f.isDirectory());
		for (File folder: subfolders) {
			BlueIndexOnDisk<ValueKey, T> index = BlueIndexOnDisk.fromExisting(collection, folder.toPath());
			String indexName = folder.getName();
			map.put(indexName, index);
		}
		return map;
	}
}
