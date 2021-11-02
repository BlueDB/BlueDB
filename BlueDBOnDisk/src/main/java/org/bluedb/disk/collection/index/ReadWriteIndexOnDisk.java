package org.bluedb.disk.collection.index;

import java.io.Serializable;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
import org.bluedb.disk.FlatMappingIterator;
import org.bluedb.disk.StreamUtils;
import org.bluedb.disk.collection.CollectionEntityIterator;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.collection.ReadableCollectionOnDisk;
import org.bluedb.disk.file.BlueObjectStreamSorter;
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
import org.bluedb.disk.serialization.BlueEntity;

public class ReadWriteIndexOnDisk<I extends ValueKey, T extends Serializable> extends ReadableIndexOnDisk<I, T> implements BlueIndex<I, T>, Rollupable {

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
		populateNewIndex(collection, index);
		return index;
	}

	private static <K extends ValueKey, T extends Serializable> void populateNewIndex(ReadableCollectionOnDisk<T> collection, ReadWriteIndexOnDisk<K, T> index) throws BlueDbException {
		Range allTime = new Range(Long.MIN_VALUE, Long.MAX_VALUE);
		try (CollectionEntityIterator<T> iterator = new CollectionEntityIterator<T>(collection.getSegmentManager(), allTime, false, Arrays.asList())) {
			while (iterator.hasNext()) {
				List<BlueEntity<T>> entities = iterator.next(1000);
				index.indexNewValues(entities);
			}
		}
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

	public void indexNewValues(Collection<BlueEntity<T>> values) throws BlueDbException {
		List<IndividualChange<BlueKey>> sortedIndexChanges = StreamUtils.stream(values)
			.flatMap(value -> StreamUtils.stream(getSortedIndexChangesForValueChange(value.getKey(), null, value.getValue())))
			.sorted()
			.collect(Collectors.toList());
		getSegmentManager().applyChanges(new InMemorySortedChangeSupplier<>(sortedIndexChanges));
	}

	public void indexChanges(SortedChangeSupplier<T> sortedChangeSupplier) throws BlueDbException {
		long changeId = nextIndexChangeId.getAndIncrement();
		Path sortedIndexChangesPath = FileUtils.createTempFilePathInDirectory(indexPath, "indexChange-" + changeId);
		try {
			SortedChangeIterator<T> sortedChangeIterator = new SortedChangeIterator<>(sortedChangeSupplier);
			FlatMappingIterator<IndividualChange<T>, IndividualChange<BlueKey>> unsortedIndexChangeIterator = new FlatMappingIterator<>(sortedChangeIterator, this::getSortedIndexChangesForValueChange);
			
			Map<BlueFileMetadataKey, String> metadataEntries = new HashMap<>();
			metadataEntries.put(BlueFileMetadataKey.SORTED_MASS_CHANGE_FILE, String.valueOf(true));
			
			BlueObjectStreamSorter<IndividualChange<BlueKey>> sorter = new BlueObjectStreamSorter<>(unsortedIndexChangeIterator, sortedIndexChangesPath, fileManager, metadataEntries);
			sorter.sortAndWriteToFile();
			
			if(FileUtils.isEmpty(sortedIndexChangesPath)) {
				return;
			}
			
			try(SortedChangeSupplier<BlueKey> onDiskSortedChangeSupplier = new OnDiskSortedChangeSupplier<>(sortedIndexChangesPath, fileManager)) {
				/*
				 * Note that a batch delete passes in null for the old value and the new value. In that case we won't 
				 * be able to calculate the proper index changes. However, this should be functioning better than it was
				 * before. It wasn't ever removing any indices, it was only adding the ones for the new values.
				 */
				getSegmentManager().applyChanges(onDiskSortedChangeSupplier);
			}
		} finally {
			sortedIndexChangesPath.toFile().delete();
		}
	}

	public void indexChange(BlueKey key, T oldValue, T newValue) throws BlueDbException {
		List<IndividualChange<BlueKey>> sortedIndexChanges = getSortedIndexChangesForValueChange(key, oldValue, newValue);
		getSegmentManager().applyChanges(new InMemorySortedChangeSupplier<>(sortedIndexChanges));
	}
	
	protected List<IndividualChange<BlueKey>> getSortedIndexChangesForValueChange(IndividualChange<T> change) {
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
}
