package org.bluedb.disk.collection.index;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.bluedb.api.CloseableIterator;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndexInfo;
import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.file.BlueObjectStreamSorter;
import org.bluedb.disk.file.FileUtils;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.serialization.BlueEntity;

public class MultiIndexCreator<T extends Serializable> {
	private final List<BlueIndexInfo<? extends ValueKey, T>> indicesToCreate;
	private final IndexManagerService<T> indexManagerService;
	private final int maxRecordsInInitializationChunks;
	
	private final List<InitializingIndex<T>> indicesToInitialize = new LinkedList<>();

	private final MultiIndexCreatorResult<T> result = new MultiIndexCreatorResult<>();
	
	public MultiIndexCreator(Collection<BlueIndexInfo<? extends ValueKey, T>> indicesToCreate, IndexManagerService<T> services) {
		this.indicesToCreate = new LinkedList<>(indicesToCreate);
		this.indexManagerService = services;
		this.maxRecordsInInitializationChunks = services.getMaxRecordsInInitializationChunks();
	}

	public MultiIndexCreatorResult<T> createIndices() {
		createIndicesAndNoteThoseNeedingInitialization();
		
		if(!indicesToInitialize.isEmpty()) {
			try {
				queueIndexChangeFilesForAllCollectionEntities();
			} catch(Throwable t) {
				return result;
			}
				
			sortAndApplyAllIndexChanges();
		}
		
		return result;
	}

	private void createIndicesAndNoteThoseNeedingInitialization() {
		runTaskForEachIndexToCreateRemovingAndNotingFailures(indexInfo -> {
			Optional<ReadWriteIndexOnDisk<ValueKey, T>> existingIndex = indexManagerService.lookupExistingIndexByName(indexInfo.getName());
			if(existingIndex.isPresent()) {
				if(existingIndex.get().needsInitialization()) {
					indicesToInitialize.add(new InitializingIndex<>(existingIndex.get(), maxRecordsInInitializationChunks));
				}
				validateRequestedIndexTypeMatchesExistingIndex(indexInfo, existingIndex.get());
			} else {
				ReadWriteIndexOnDisk<ValueKey, T> index = indexManagerService.createNewIndex(indexInfo.getName(), indexInfo.getKeyExtractor());
				indicesToInitialize.add(new InitializingIndex<>(index, maxRecordsInInitializationChunks));
				result.addNewlyCreatedIndex(index);
			}
		});
	}

	private void validateRequestedIndexTypeMatchesExistingIndex(BlueIndexInfo<? extends ValueKey, T> requestedIndex, ReadWriteIndexOnDisk<? extends ValueKey, T> existingIndex) throws BlueDbException {
		if (existingIndex.getType() != requestedIndex.getKeyType()) {
			throw new BlueDbException("Invalid type (" + requestedIndex.getKeyType() + ") for existing index " + requestedIndex.getName() + " of type " + existingIndex.getType());
		}
	}

	private void queueIndexChangeFilesForAllCollectionEntities() throws BlueDbException {
		try(CloseableIterator<BlueEntity<T>> entityIterator = indexManagerService.getEntityIteratorForEntireCollection()) {
			while(entityIterator.hasNext()) {
				BlueEntity<T> nextEntity = entityIterator.next();
				IndividualChange<T> valueChange = IndividualChange.createInsertChange(nextEntity.getKey(), nextEntity.getValue());
				
				runTaskForEachInitializingIndexRemovingAndNotingFailures(index -> {
					index.queueIndexChangesForNextEntity(valueChange);
				});
			}
		} catch(Throwable t) {
			handleFailureForAllIndices(t);
			throw t;
		}
	}

	private void sortAndApplyAllIndexChanges() {
		runTaskForEachInitializingIndexRemovingAndNotingFailures(index -> {
			index.sort();
			index.applyChanges();
			index.cleanupChangeFile();
		});
	}
	
	public static class MultiIndexCreatorResult<T extends Serializable> {
		private final List<ReadWriteIndexOnDisk<ValueKey, T>> newlyCreatedIndices = new LinkedList<>();
		private final Set<String> failedIndexNames = new HashSet<>();
		
		public List<ReadWriteIndexOnDisk<ValueKey, T>> getNewlyCreatedIndices() {
			return newlyCreatedIndices;
		}
		
		public void addNewlyCreatedIndex(ReadWriteIndexOnDisk<ValueKey, T> index) {
			newlyCreatedIndices.add(index);
		}
		
		public Set<String> getFailedIndexNames() {
			return failedIndexNames;
		}
		
		public void addFailedIndexName(String indexName) {
			failedIndexNames.add(indexName);
		}
	}

	private void runTaskForEachIndexToCreateRemovingAndNotingFailures(IndexInfoTask<T> task) {
		Iterator<BlueIndexInfo<? extends ValueKey, T>> it = indicesToCreate.iterator();
		while(it.hasNext()) {
			BlueIndexInfo<? extends ValueKey, T> indexInfo = it.next();
			try {
				task.execute(indexInfo);
			} catch(Throwable t) {
				handleIndexFailure(indexInfo.getName(), t);
				it.remove();
			}
		}
	}

	private void runTaskForEachInitializingIndexRemovingAndNotingFailures(InitializingIndexTask<T> task) {
		Iterator<InitializingIndex<T>> it = indicesToInitialize.iterator();
		while(it.hasNext()) {
			InitializingIndex<T> index = it.next();
			try {
				task.execute(index);
			} catch(Throwable t) {
				handleIndexFailure(index, t);
				it.remove();
			}
		}
	}

	private void handleIndexFailure(InitializingIndex<T> index, Throwable t) {
		handleIndexFailure(index.getName(), t);
		index.cleanupChangeFile();
	}

	private void handleIndexFailure(String indexName, Throwable t) {
		t.printStackTrace();
		result.addFailedIndexName(indexName);
	}

	private void handleFailureForAllIndices(Throwable t) {
		t.printStackTrace();
		for(InitializingIndex<T> index : indicesToInitialize) {
			result.addFailedIndexName(index.getName());
			index.cleanupChangeFile();
		}
	}
	
	public interface IndexManagerService<T extends Serializable> {
		public Optional<ReadWriteIndexOnDisk<ValueKey, T>> lookupExistingIndexByName(String indexName) throws BlueDbException;

		public ReadWriteIndexOnDisk<ValueKey, T> createNewIndex(String name, KeyExtractor<? extends ValueKey, T> keyExtractor) throws BlueDbException;
		
		public CloseableIterator<BlueEntity<T>> getEntityIteratorForEntireCollection() throws BlueDbException;
		
		/** To initialize an index we must go through all entities in the collection and generate and sort index changes. That
		 * process involves sorting on disk. This value controls how many records are in each initial file on disk before being
		 * combined. */
		public int getMaxRecordsInInitializationChunks();
	}

	private static interface InitializingIndexTask<T extends Serializable> {
		public void execute(InitializingIndex<T> index) throws BlueDbException;
	}

	private static interface IndexInfoTask<T extends Serializable> {
		public void execute(BlueIndexInfo<? extends ValueKey, T> indexInfo) throws BlueDbException;
	}
	
	private static class InitializingIndex<T extends Serializable> {
		private final ReadWriteIndexOnDisk<ValueKey, T> index;
		private final List<IndividualChange<BlueKey>> queuedIndexChanges;
		private final Path sortedIndexChangesPath;
		private final BlueObjectStreamSorter<IndividualChange<BlueKey>> sorter;
		private final int maxRecordsInInitialChunks;
		
		public InitializingIndex(ReadWriteIndexOnDisk<ValueKey, T> index, int maxRecordsInInitialChunks) throws BlueDbException {
			this.index = index;
			this.queuedIndexChanges = new LinkedList<>();
			this.sortedIndexChangesPath = index.createNextIndexChangeStoragePath();
			this.sorter = index.createBlueObjectStreamSorter(null, sortedIndexChangesPath);
			this.maxRecordsInInitialChunks = maxRecordsInInitialChunks;
		}

		public String getName() {
			return index.getIndexName();
		}

		public void queueIndexChangesForNextEntity(IndividualChange<T> valueChange) throws BlueDbException {
			queuedIndexChanges.addAll(index.getSortedIndexChangesForValueChange(valueChange));
			if(queuedIndexChanges.size() >= maxRecordsInInitialChunks) {
				flushQueuedIndexChanges();
			}
		}

		private void flushQueuedIndexChanges() throws BlueDbException {
			if(queuedIndexChanges.size() > 0) {
				sorter.addBatchOfObjectsToBeSorted(queuedIndexChanges);
				queuedIndexChanges.clear();
			}
		}

		public void sort() throws BlueDbException {
			flushQueuedIndexChanges();
			sorter.sortAndWriteToFile();
		}
		
		public void applyChanges() throws BlueDbException {
			index.applyIndexChanges(sortedIndexChangesPath);
			index.markInitializationComplete();
		}
		
		public void cleanupChangeFile() {
			try {
				FileUtils.deleteIfExistsWithoutLock(sortedIndexChangesPath);
			} catch(Throwable t) {
				t.printStackTrace();
			}
		}
		
	}
	
}
