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
		segmentManager = new ReadWriteSegmentManager<BlueKey>(indexPath, fileManager, this, sizeSetting.getConfig(), false);
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
		List<IndividualChange<BlueKey>> sortedIndexChanges = getSortedIndexChangesForValueChange(key, key, false, oldValue, newValue);
		getSegmentManager().applyChanges(new InMemorySortedChangeSupplier<>(sortedIndexChanges));
	}
	
	public List<IndividualChange<BlueKey>> getSortedIndexChangesForValueChange(IndividualChange<T> change) {
		return getSortedIndexChangesForValueChange(change.getOriginalKey(), change.getKey(), change.isKeyChanged(), change.getOldValue(), change.getNewValue());
	}

	protected List<IndividualChange<BlueKey>> getSortedIndexChangesForValueChange(BlueKey key, T oldValue, T newValue) {
		return getSortedIndexChangesForValueChange(key, key, false, oldValue, newValue);
	}

	protected List<IndividualChange<BlueKey>> getSortedIndexChangesForValueChange(BlueKey originalValueKey, BlueKey newValueKey, boolean isKeyChanged, T oldValue, T newValue) {
		Set<IndexCompositeKey<I>> oldCompositeKeys = StreamUtils.stream(toCompositeKeys(originalValueKey, oldValue))
				.collect(Collectors.toCollection(HashSet::new));
		
		Set<IndexCompositeKey<I>> newCompositeKeys = StreamUtils.stream(toCompositeKeys(newValueKey, newValue))
				.collect(Collectors.toCollection(HashSet::new));
		
		Stream<IndividualChange<BlueKey>> deleteChanges = StreamUtils.stream(oldCompositeKeys)
			.filter(oldIndexKey -> !newCompositeKeys.contains(oldIndexKey))
			.map(oldIndexKey -> IndividualChange.createDeleteChange(oldIndexKey, originalValueKey));
		
		Stream<IndividualChange<BlueKey>> insertChanges = StreamUtils.stream(newCompositeKeys)
				.filter(newIndexKey -> isKeyChanged || !oldCompositeKeys.contains(newIndexKey)) //If the index key is new for this value key or the value key changed then we want to update our index data for it to reflect the new value key
				.map(newIndexKey -> IndividualChange.createInsertChange(newIndexKey, newValueKey)); //An insert change is fine for index files. We don't need to know the previous value for index changes.
		
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
		
		return StreamUtils.stream(extractIndexKeys(destination, value))
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
