package org.bluedb.disk.collection.index;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.bluedb.api.CloseableIterator;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.BlueIndexInfo;
import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.collection.index.MultiIndexCreator.MultiIndexCreatorResult;
import org.bluedb.disk.collection.index.MultiIndexCreator.IndexManagerService;
import org.bluedb.disk.file.FileUtils;
import org.bluedb.disk.file.BlueObjectStreamSorter.BlueObjectStreamSorterConfig;
import org.bluedb.disk.query.QueryOnDisk;
import org.bluedb.disk.recovery.SortedChangeSupplier;
import org.bluedb.disk.serialization.BlueEntity;

public class ReadWriteIndexManager<T extends Serializable> extends ReadableIndexManager<T> implements IndexManagerService<T> {

	private final ReadWriteCollectionOnDisk<T> collection;
	private Map<String, ReadWriteIndexOnDisk<ValueKey, T>> indexesByName;

	public ReadWriteIndexManager(ReadWriteCollectionOnDisk<T> collection, Path collectionPath) throws BlueDbException {
		this.collection = collection;
		indexesByName = getIndexesFromDisk(collection, collectionPath);
	}

	public <K extends ValueKey> BlueIndex<K, T> getOrCreate(String indexName, Class<K> keyType, KeyExtractor<K, T> keyExtractor) throws BlueDbException {
		List<BlueIndexInfo<? extends ValueKey, T>> indexInfoList = new LinkedList<>();
		indexInfoList.add(new BlueIndexInfo<>(indexName, keyType, keyExtractor));
		createIndices(indexInfoList);
		return getIndex(indexName, keyType);
	}
	
	public void createIndices(Collection<BlueIndexInfo<? extends ValueKey, T>> indicesToCreate) throws BlueDbException {
		MultiIndexCreator<T> multiIndexCreator = new MultiIndexCreator<>(indicesToCreate, this);
		MultiIndexCreatorResult<T> result = multiIndexCreator.createIndices();
		for(ReadWriteIndexOnDisk<ValueKey, T> index : result.getNewlyCreatedIndices()) {
			indexesByName.put(index.getIndexName(), index);	
		}
		
		if(!result.getFailedIndexNames().isEmpty()) {
			throw new BlueDbException("Failed to create " + result.getFailedIndexNames().size() + " indices. See corresponding exceptions for each above in the logs. Failed indices: " + result.getFailedIndexNames());
		}
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

	public void indexChanges(SortedChangeSupplier<T> sortedChangeSupplier) throws BlueDbException {
		for (ReadWriteIndexOnDisk<ValueKey, T> index: indexesByName.values()) {
			index.indexChanges(sortedChangeSupplier);
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
	

	@Override
	public Optional<ReadWriteIndexOnDisk<ValueKey, T>> lookupExistingIndexByName(String indexName) throws BlueDbException {
		return Optional.ofNullable(indexesByName.get(indexName));
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public ReadWriteIndexOnDisk<ValueKey, T> createNewIndex(String indexName, KeyExtractor<? extends ValueKey, T> keyExtractor) throws BlueDbException {
		Path indexPath = Paths.get(collection.getPath().toString(), INDEXES_SUBFOLDER, indexName);
		return (ReadWriteIndexOnDisk<ValueKey, T>) ReadWriteIndexOnDisk.createNew(collection, indexPath, keyExtractor);
	}

	@Override
	public CloseableIterator<BlueEntity<T>> getEntityIteratorForEntireCollection() throws BlueDbException {
		return new QueryOnDisk<>(collection).getEntityIterator();
	}

	@Override
	public int getMaxRecordsInInitializationChunks() {
		return BlueObjectStreamSorterConfig.createDefault().maxRecordsInInitialChunks;
	}
	
}
