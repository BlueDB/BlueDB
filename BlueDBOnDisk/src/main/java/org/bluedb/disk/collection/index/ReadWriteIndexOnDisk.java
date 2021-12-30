package org.bluedb.disk.collection.index;

import java.io.Serializable;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.IteratorWrapper;
import org.bluedb.disk.IteratorWrapper.IteratorWrapperFlatMapper;
import org.bluedb.disk.StreamUtils;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.file.BlueObjectStreamSorter;
import org.bluedb.disk.file.BlueObjectStreamSorter.BlueObjectStreamSorterConfig;
import org.bluedb.disk.file.FileUtils;
import org.bluedb.disk.file.ReadWriteFileManager;
import org.bluedb.disk.metadata.BlueFileMetadataKey;
import org.bluedb.disk.recovery.InMemorySortedChangeSupplier;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.recovery.OnDiskSortedChangeSupplier;
import org.bluedb.disk.recovery.SortedChangeIterator;
import org.bluedb.disk.recovery.SortedChangeSupplier;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadWriteSegment;
import org.bluedb.disk.segment.ReadWriteSegmentManager;
import org.bluedb.disk.segment.SegmentSizeSetting;
import org.bluedb.disk.segment.rollup.IndexRollupTarget;
import org.bluedb.disk.segment.rollup.RollupScheduler;
import org.bluedb.disk.segment.rollup.RollupTarget;
import org.bluedb.disk.segment.rollup.Rollupable;

/*
 * BlueDB next steps:
 * 1 - Retroactively creating index files should do a single pass over the collection (one pass for all indices) and should
 * be recoverable if the system stops before it completes.
 * 2 - Add index conditions to the BlueDB API.
 * 3 - Automatically add index conditions for start and end times and use that to support timeframe collection queries instead of with duplicated
 * records.
 *   3b - How do we deal with legacy databases and the possibility of someone downgrading? Not adding duplicate record files or deleting
 *   old duplicate record files will make it so that old BlueDB versions won't work anymore...
 * 
 * 
 * 
 * 
 * Chronicall Updates:
 * 1 - Feature event ending process needs updating. It should do everything in a single batch now instead of in batches of 1000
 * 
 * 
 * 
 * 
 * Old Notes:
 * 2 - Retroactively creating index files should only iterate through the collection once. It should also create a pending 
 * change file so that it is guaranteed to complete. However, consider updating the pending change file as it progresses
 * so that it doesn't have to redo work after a crash.
 * 3 - Investigate a way to allow super long write queries to give up the thread periodically to allow other queries
 * to run.
 *     - GroupThreadPool Changes:
 *       1 - Instead of submitting runnables, submit QueryTask objects.
 *       2 - QueryTask objects need to do things in chunks. After completing a chunk, a task should be in a good place
 *       to pause and give up the thread. If the task is set to give up the thread periodically then it should check
 *       if it should stop after each chunk and resume when told to resume. It should never stop while holding a lock.
 *       3 - The cached thread pool should have a runnable submitted which contains a queue of the queries to run. It should 
 *       manage which query is currently running and the pausing and resuming of queries to avoid starvation of queries.
 *       4 - If there are no queries waiting to run for a particular collection then the runnable completes and allows the
 *       thread to be used by another group or to be garbage collected.
 *       5 - Prioritize creating pending change files to ensure that queries get executed on startup even if the service
 *       crashed before previous write queries completed. This should help to avoid issues where you think a query ran
 *       but it was starved until a crash. Note, that queries that don't give up the thread should stop new pending change
 *       files from being written in order to avoid creating pending change files that will be out of date when the query
 *       completes. Does this mean that we can create pending change files on a separate thread if there isn't a blocking
 *       query ahead of it? I think that we could, which could save some serious time. Finding and documenting all of the
 *       changes is a read only query, but needs to be done after blocking queries complete to be up to date. If someone really
 *       needs to make ordered writes then they should synchronize it themselves by calling them all from a single thread or
 *       something like that. What are our reasons for limiting one write pre thread, might be worth considering relaxing
 *       that unless blocking is explicitly requested.
 *       6 - We could separate the creation of pending change files, the updating of collections, and the updating of indices
 *       to each be on its own thread. We'd have to guarantee that write queries go through each phase in order and we'd have
 *       to utilize information in pending change files to avoid query 2 accidentally undoing a change that query 1 is making.
 *       The pending change step would probably want to create change files for collection and index changes as part of phase 1.
 *       We can't do more than that without opening ourselves up to weird inconsistency errors... What if we automatically broke
 *       up pending change files and those that are not in progress could be combine with new ones that come in after them. That
 *       would allow a later query to be ran in tandem with a currently runnign query... very interesting.
 * 5 - Add index condition support to queries that allow us to utilize an index to narrow down what segments need to
 * be searched before executing the query. This would be super powerful. We could even automatically utilize time indexes 
 * on TimeFrameKey collections in order to negate the need for having duplicate file chunks in each overlapping segment.
 * 6 - Pending Change Plan. Goal, to do less passes over the collection by combining/batching write queries. Ideally, we
 * want to prioritize data with higher grouping numbers.
 *     Phase 1 - Write queries that are queued up are added to a pending passive change file in a single pass.
 *        a - They wait for the write thread before starting this process.
 *        b - We grab all waiting queries and do a single pass over the collection for all of them. We also simultaneously 
 *        do a pass over the pending massive change file. When we find an object to update, we use the object from the
 *        pending change file if one exists, otherwise the one from the collection. We apply changes for each query in
 *        the order they were executed. The pending massive change file is re-written with up to date changes for each
 *        object involved in each query.
 *        c - We should have a pending index changes file for each index that is also updated as part of the b process.
 *        d - Mass pending change files should be split into different files representing chunks that can be done on 
 *        their own. That way we can prioritize the changes that only involve recent timeframes and work our way backwards.
 *        A query will know which pending change files it needs to wait for completion on before returning. Therefore, Query B
 *        could return before Query A if Query B was scheduled after but only updated a single item from today and Query A was
 *        a large update spanning a large timeframe. However, if the object was updated by both queries then the object would
 *        have had Query A's change made before Query B's change.
 *        e - A change will be located in the pending change file based on its start time, not based on its end time.
 *        f - (alternate option) - Keep changes in a single file. When its time to run them, analyze it and decide what 
 *        timeframe to execute. Execute it, and write the changes to a completed changes file, removing them from the 
 *        pending changes file. We'd need some way to avoid complete starvation of big queries. We'd have to force it
 *        to work its way backwards so that constant queries from today don't stop it from ever getting back far 
 *        enough. We should be able to make progress on big queries while keeping up with new queries.
 *        f - a - Each query would have to know its effective grouping number range so that it can return when all
 *          of its changes are completed. In fact, it isn't really important to run part of a query. Each pass should
 *          result in at least one query being completed. And if we have records that span the entire range then we'll
 *          just have to execute the whole thing before moving on.
 *     Phase 2 - The pending change files (collection and indices) are executed, starting with the most recent ones
 *     and working our way backwards. We need to make sure not to completely starve the old pending change files though.
 *        a - Once a pending change file starts, we must complete it. Though it will complete all in a single operation.
 *        b - The pending change file will then get marked as completed.
 *        c - Phase 1 should run to completion every time we finish one pending change file.
 *     Backups - The backup will still copy pending change files that were created while the backup was running.
 *     Recovery - We don't have to run all pending changes before starting up anymore. We can execute them along
 *       with new queries. 
 * 7 - Add a way to update the end time on a timeframe object. It would have to update the object in each segment it already
 * exists in and insert the object into any segment that it now should exist in. Technically you could have to delete it from
 * segments that it shouldn't exist in anymore. 
 * 8 - Figure out a way to make the end time of a TimeFrameKey not required in order to access an object... It is definitely
 * awkward to have to know the end time. Those records need to have an end time associated with them, but if we can stop
 * duplicating those records then we can probably only use it for the auto generated index.
 * 9 - Add new grouping number generation algorithms that are more equitably distributed. Most seem to get clumped in the middle, which is not ideal.
 */

public class ReadWriteIndexOnDisk<I extends ValueKey, T extends Serializable> extends ReadableIndexOnDisk<I, T> implements BlueIndex<I, T>, Rollupable {
	protected final static String FILE_KEY_NEEDS_INITIALIZING = ".needs-initialization";

	private final RollupScheduler rollupScheduler;
	private final String indexName;
	private final ReadWriteSegmentManager<BlueKey> segmentManager;
	private final ReadWriteFileManager fileManager;
	
	private AtomicLong nextIndexChangeId = new AtomicLong(0);

	public static <K extends ValueKey, T extends Serializable> ReadWriteIndexOnDisk<K, T> createNew(ReadWriteCollectionOnDisk<T> collection, Path indexPath, KeyExtractor<K, T> keyExtractor) throws BlueDbException {
		indexPath.toFile().mkdirs();
		ReadWriteFileManager fileManager = collection.getFileManager();
		Path keyExtractorPath = Paths.get(indexPath.toString(), FILE_KEY_EXTRACTOR);
		fileManager.saveObject(keyExtractorPath, keyExtractor);
		ReadWriteIndexOnDisk<K, T> index = new ReadWriteIndexOnDisk<K, T>(collection, indexPath, keyExtractor);
		index.markAsNeedsInitialization();
		return index;
	}

	public static <K extends ValueKey, T extends Serializable> ReadWriteIndexOnDisk<K, T> fromExisting(ReadWriteCollectionOnDisk<T> collection, Path indexPath) throws BlueDbException {
		ReadWriteFileManager fileManager = collection.getFileManager();
		Path keyExtractorPath = Paths.get(indexPath.toString(), FILE_KEY_EXTRACTOR);
		@SuppressWarnings("unchecked")
		KeyExtractor<K, T> keyExtractor = (KeyExtractor<K, T>) fileManager.loadObject(keyExtractorPath);
		return new ReadWriteIndexOnDisk<K, T>(collection, indexPath, keyExtractor);
	}

	private ReadWriteIndexOnDisk(ReadWriteCollectionOnDisk<T> collection, Path indexPath, KeyExtractor<I, T> keyExtractor) throws BlueDbException {
		super(collection, indexPath, keyExtractor);
		this.indexName = indexPath.toFile().getName();
		this.fileManager = collection.getFileManager();
		SegmentSizeSetting sizeSetting = determineSegmentSize(keyExtractor.getType());
		segmentManager = new ReadWriteSegmentManager<BlueKey>(indexPath, fileManager, this, sizeSetting.getConfig());
		rollupScheduler = collection.getRollupScheduler();
		cleanupTempFiles();
	}
	
	public String getIndexName() {
		return indexName;
	}

	public ReadWriteSegmentManager<BlueKey> getSegmentManager() {
		return segmentManager;
	}

	@Override
	public void reportReads(List<RollupTarget> rollupTargets) {
		List<IndexRollupTarget> indexRollupTargets = toIndexRollupTargets(rollupTargets);
		rollupScheduler.reportReads(indexRollupTargets);
	}

	@Override
	public void reportWrites(List<RollupTarget> rollupTargets) {
		List<IndexRollupTarget> indexRollupTargets = toIndexRollupTargets(rollupTargets);
		rollupScheduler.reportWrites(indexRollupTargets);
	}

	private List<IndexRollupTarget> toIndexRollupTargets(List<RollupTarget> rollupTargets) {
		return rollupTargets.stream()
				.map( this::toIndexRollupTarget )
				.collect( Collectors.toList() );
	}

	private IndexRollupTarget toIndexRollupTarget(RollupTarget rollupTarget) {
		return new IndexRollupTarget(indexName, rollupTarget.getSegmentGroupingNumber(), rollupTarget.getRange() );
	}

	public void indexChanges(SortedChangeSupplier<T> sortedChangeSupplier) throws BlueDbException {
		Path sortedIndexChangesPath = createNextIndexChangeStoragePath();
		try {
			SortedChangeIterator<T> sortedChangeIterator = new SortedChangeIterator<>(sortedChangeSupplier);
			IteratorWrapperFlatMapper<IndividualChange<T>, IndividualChange<BlueKey>> flatMapper = this::getSortedIndexChangesForValueChange;
			Iterator<IndividualChange<BlueKey>> unsortedIndexChangeIterator = new IteratorWrapper<>(sortedChangeIterator, flatMapper);
			
			Map<BlueFileMetadataKey, String> metadataEntries = new HashMap<>();
			metadataEntries.put(BlueFileMetadataKey.SORTED_MASS_CHANGE_FILE, String.valueOf(true));
			
			BlueObjectStreamSorter<IndividualChange<BlueKey>> sorter = createBlueObjectStreamSorter(unsortedIndexChangeIterator, sortedIndexChangesPath);
			sorter.sortAndWriteToFile();
			
			applyIndexChanges(sortedIndexChangesPath);
		} finally {
			sortedIndexChangesPath.toFile().delete();
		}
	}
	
	public Path createNextIndexChangeStoragePath() throws BlueDbException {
		long changeId = nextIndexChangeId.getAndIncrement();
		return FileUtils.createTempFilePathInDirectory(indexPath, "indexChange-" + changeId);
	}
	
	public BlueObjectStreamSorter<IndividualChange<BlueKey>> createBlueObjectStreamSorter(Iterator<IndividualChange<BlueKey>> unsortedIndexChangeIterator, Path sortedIndexChangesPath) {
		Map<BlueFileMetadataKey, String> metadataEntries = new HashMap<>();
		metadataEntries.put(BlueFileMetadataKey.SORTED_MASS_CHANGE_FILE, String.valueOf(true));
		
		return new BlueObjectStreamSorter<>(unsortedIndexChangeIterator, sortedIndexChangesPath, fileManager, metadataEntries, BlueObjectStreamSorterConfig.createDefault());
	}
	
	public void applyIndexChanges(Path sortedIndexChangesPath) throws BlueDbException {
		if(FileUtils.isEmpty(sortedIndexChangesPath)) {
			return;
		}
		
		try(SortedChangeSupplier<BlueKey> onDiskSortedChangeSupplier = new OnDiskSortedChangeSupplier<>(sortedIndexChangesPath, fileManager)) {
			getSegmentManager().applyChanges(onDiskSortedChangeSupplier);
		}
	}

	public void indexChange(BlueKey key, T oldValue, T newValue) throws BlueDbException {
		List<IndividualChange<BlueKey>> sortedIndexChanges = getSortedIndexChangesForValueChange(key, oldValue, newValue);
		getSegmentManager().applyChanges(new InMemorySortedChangeSupplier<>(sortedIndexChanges));
	}
	
	public List<IndividualChange<BlueKey>> getSortedIndexChangesForValueChange(IndividualChange<T> change) {
		return getSortedIndexChangesForValueChange(change.getKey(), change.getOldValue(), change.getNewValue());
	}

	protected List<IndividualChange<BlueKey>> getSortedIndexChangesForValueChange(BlueKey valueKey, T oldValue, T newValue) {
		Set<IndexCompositeKey<I>> oldCompositeKeys = StreamUtils.stream(toCompositeKeys(valueKey, oldValue))
				.collect(Collectors.toCollection(HashSet::new));
		
		Set<IndexCompositeKey<I>> newCompositeKeys = StreamUtils.stream(toCompositeKeys(valueKey, newValue))
				.collect(Collectors.toCollection(HashSet::new));
		
		Stream<IndividualChange<BlueKey>> deleteChanges = StreamUtils.stream(oldCompositeKeys)
			.filter(oldKey -> !newCompositeKeys.contains(oldKey))
			.map(oldKey -> new IndividualChange<>(oldKey, valueKey, null));
		
		Stream<IndividualChange<BlueKey>> insertChanges = StreamUtils.stream(newCompositeKeys)
				.filter(newKey -> !oldCompositeKeys.contains(newKey))
				.map(newKey -> IndividualChange.createInsertChange(newKey, valueKey));
		
		return StreamUtils.concat(deleteChanges, insertChanges)
				.sorted()
				.collect(Collectors.toList());
	}

	public void rollup(Range range) throws BlueDbException {
		ReadWriteSegment<?> segment = getSegmentManager().getSegment(range.getStart());
		segment.rollup(range);
	}

	private List<IndexCompositeKey<I>> toCompositeKeys(BlueKey destination, T value) {
		if(value == null) {
			return new ArrayList<>();
		}
		
		return StreamUtils.stream(keyExtractor.extractKeys(value))
				.map( (indexKey) -> new IndexCompositeKey<I>(indexKey, destination) )
				.collect( Collectors.toList() );
	}

	protected void cleanupTempFiles() {
		try {
			DirectoryStream<Path> tempIndexFileStream = FileUtils.getTempFolderContentsAsStream(indexPath.toFile(), file -> true);
			tempIndexFileStream.forEach(path -> {
				path.toFile().delete();	
			});
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	protected void markAsNeedsInitialization() throws BlueDbException {
		Boolean needsInitialization = true;
		Path needsInitializationPath = indexPath.resolve(FILE_KEY_NEEDS_INITIALIZING);
		fileManager.saveObject(needsInitializationPath, needsInitialization);
	}

	public boolean needsInitialization() throws BlueDbException {
		Path needsInitializationPath = indexPath.resolve(FILE_KEY_NEEDS_INITIALIZING);
		if(!FileUtils.isEmpty(needsInitializationPath)) {
			Boolean needsInitialization = (Boolean) fileManager.loadObject(needsInitializationPath);
			if(needsInitialization != null) {
				return needsInitialization;
			}
		}
		return false; //Legacy index collections won't have this file but do not need to be initialized
	}

	public void markInitializationComplete() throws BlueDbException {
		Boolean needsInitialization = false;
		Path needsInitializationPath = indexPath.resolve(FILE_KEY_NEEDS_INITIALIZING);
		fileManager.saveObject(needsInitializationPath, needsInitialization);
	}
}
