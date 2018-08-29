package io.bluedb.disk.collection.index;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import io.bluedb.api.BlueIndex;
import io.bluedb.api.KeyExtractor;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.file.FileManager;

public class IndexManager<T extends Serializable> {

	private static final String INDEXES_SUBFOLDER = ".index";

	private final BlueCollectionOnDisk<T> collection;
	private Map<String, BlueIndexOnDisk<BlueKey, T>> indexesByName;

	public IndexManager(BlueCollectionOnDisk<T> collection, Path collectionPath) throws BlueDbException {
		this.collection = collection;
		indexesByName = getIndexesFromDisk(collection, collectionPath);
	}

	public <K extends BlueKey> BlueIndex<K, T> createIndex(String indexName, Class<K> keyType, KeyExtractor<K, T> keyExtractor) throws BlueDbException {
		Path indexPath = Paths.get(collection.getPath().toString(), INDEXES_SUBFOLDER, indexName);
		BlueIndexOnDisk<K, T> index = BlueIndexOnDisk.createNew(collection, indexPath, keyExtractor);
		indexesByName.put(indexName, (BlueIndexOnDisk<BlueKey, T>) index);
		return index;
	}

	public <K extends BlueKey> BlueIndex<K, T> getIndex(String indexName, Class<K> keyType) throws BlueDbException {
		BlueIndex<BlueKey, T> index = indexesByName.get(indexName);
		// TODO make sure type is correct
		return (BlueIndex<K, T>) index;
	}

	public void removeFromAllIndexes(BlueKey key, T value) throws BlueDbException {
		for (BlueIndexOnDisk<BlueKey, T> index: indexesByName.values()) {
			index.remove(key, value);
		}
		// TODO remember to handle nulls
	}

	public void addToAllIndexes(BlueKey key, T value) throws BlueDbException {
		for (BlueIndexOnDisk<BlueKey, T> index: indexesByName.values()) {
			index.add(key,  value);
		}
		// TODO remember to handle nulls
	}

	protected Map<String, BlueIndexOnDisk<BlueKey, T>> getIndexesFromDisk(BlueCollectionOnDisk<T> collection, Path collectionPath) throws BlueDbException {
		Map<String, BlueIndexOnDisk<BlueKey, T>> map = new HashMap<>();
		Path indexesPath = Paths.get(collectionPath.toString(), INDEXES_SUBFOLDER);
		List<File> subfolders = FileManager.getFolderContents(indexesPath.toFile(), (f) -> f.isDirectory());
		for (File folder: subfolders) {
			BlueIndexOnDisk<BlueKey, T> index = BlueIndexOnDisk.fromExisting(collection, folder.toPath());
			String indexName = folder.getName();
			map.put(indexName, index);
		}
		return map;
	}
}
