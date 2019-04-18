package io.bluedb.disk.collection.index;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.index.BlueIndex;
import io.bluedb.api.index.KeyExtractor;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.ValueKey;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.Blutils.CheckedFunction;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.collection.CollectionEntityIterator;
import io.bluedb.disk.collection.LastEntityFinder;
import io.bluedb.disk.file.FileManager;
import io.bluedb.disk.recovery.IndividualChange;
import io.bluedb.disk.recovery.PendingBatchChange;
import io.bluedb.disk.segment.Range;
import io.bluedb.disk.segment.Segment;
import io.bluedb.disk.segment.SegmentManager;
import io.bluedb.disk.segment.rollup.IndexRollupTarget;
import io.bluedb.disk.segment.rollup.RollupTarget;
import io.bluedb.disk.segment.rollup.Rollupable;
import io.bluedb.disk.serialization.BlueEntity;

public class BlueIndexOnDisk<I extends ValueKey, T extends Serializable> implements BlueIndex<I, T>, Rollupable {

	private final static String FILE_KEY_EXTRACTOR = ".extractor";

	private final BlueCollectionOnDisk<T> collection;
	private final KeyExtractor<I, T> keyExtractor;
	private final FileManager fileManager;
	private final SegmentManager<BlueKey> segmentManager;
	private final String indexName;

	public static <K extends ValueKey, T extends Serializable> BlueIndexOnDisk<K, T> createNew(BlueCollectionOnDisk<T> collection, Path indexPath, KeyExtractor<K, T> keyExtractor) throws BlueDbException {
		indexPath.toFile().mkdirs();
		FileManager fileManager = collection.getFileManager();
		Path keyExtractorPath = Paths.get(indexPath.toString(), FILE_KEY_EXTRACTOR);
		fileManager.saveObject(keyExtractorPath, keyExtractor);
		return new BlueIndexOnDisk<K, T>(collection, indexPath, keyExtractor);
	}

	public static <K extends ValueKey, T extends Serializable> BlueIndexOnDisk<K, T> fromExisting(BlueCollectionOnDisk<T> collection, Path indexPath) throws BlueDbException {
		FileManager fileManager = collection.getFileManager();
		Path keyExtractorPath = Paths.get(indexPath.toString(), FILE_KEY_EXTRACTOR);
		@SuppressWarnings("unchecked")
		KeyExtractor<K, T> keyExtractor = (KeyExtractor<K, T>) fileManager.loadObject(keyExtractorPath);
		return new BlueIndexOnDisk<K, T>(collection, indexPath, keyExtractor);
	}

	public Class<I> getType() {
		return keyExtractor.getType();
	}

	private BlueIndexOnDisk(BlueCollectionOnDisk<T> collection, Path indexPath, KeyExtractor<I, T> keyExtractor) {
		this.collection = collection;
		this.keyExtractor = keyExtractor;
		this.fileManager = collection.getFileManager();
		this.indexName = indexPath.toFile().getName();
		segmentManager = new SegmentManager<BlueKey>(indexPath, fileManager, this, keyExtractor.getType());
	}

	public void add(BlueKey key, T newItem) throws BlueDbException {
		if (newItem == null) {
			return;
		}
		for (IndexCompositeKey<I> compositeKey: toCompositeKeys(key, newItem)) {
			Segment<BlueKey> segment = segmentManager.getFirstSegment(compositeKey);
			segment.insert(compositeKey, key);
		}
	}

	public void add(Collection<IndividualChange<T>> changes) throws BlueDbException {
		List<IndividualChange<BlueKey>> indexChanges = toIndexChanges(changes);
		Collections.sort(indexChanges);
		PendingBatchChange.apply(segmentManager, indexChanges);
	}

	public void remove(BlueKey key, T oldItem) throws BlueDbException {
		if (oldItem == null) {
			return;
		}
		for (IndexCompositeKey<I> compositeKey: toCompositeKeys(key, oldItem)) {
			Segment<BlueKey> segment = segmentManager.getFirstSegment(compositeKey);
			segment.delete(compositeKey);
		}
	}

	@Override
	public List<T> get(I key) throws BlueDbException {
		List<BlueKey> underlyingKeys = getKeys(key);
		List<T> values = Blutils.map(underlyingKeys, (k) -> collection.get(k));
		return values;
	}

	public List<BlueKey> getKeys(I key) {
		Range range = new Range(key.getGroupingNumber(), key.getGroupingNumber());
		List<BlueKey> keys = new ArrayList<>();
		try (CollectionEntityIterator<BlueKey> entityIterator = new CollectionEntityIterator<>(segmentManager, range, true, new ArrayList<>())) {
			while (entityIterator.hasNext()) {
				@SuppressWarnings("unchecked")
				IndexCompositeKey<I> indexKey = (IndexCompositeKey<I>) entityIterator.next().getKey();
				keys.add(indexKey.getValueKey());
			}
		}
		return keys;
	}

	public void rollup(Range range) throws BlueDbException {
		Segment<?> segment = segmentManager.getSegment(range.getStart());
		segment.rollup(range);
	}


	public SegmentManager<BlueKey> getSegmentManager() {
		return segmentManager;
	}

	private List<IndividualChange<BlueKey>> toIndexChanges(Collection<IndividualChange<T>> changes) {
		return changes.stream()
				.map( (IndividualChange<T> change) -> toIndexChanges(change) )
				.flatMap(List::stream).collect(Collectors.toList());
	}

	private List<IndividualChange<BlueKey>> toIndexChanges(IndividualChange<T> change) {
		List<IndexCompositeKey<I>> compositeKeys = toCompositeKeys(change);
		BlueKey underlyingKey = change.getKey();
		return toIndexChanges(compositeKeys, underlyingKey);
	}

	private static <I extends ValueKey> List<IndividualChange<BlueKey>> toIndexChanges(List<IndexCompositeKey<I>> compositeKeys, BlueKey destinationKey) {
		List<IndividualChange<BlueKey>> indexChanges = new ArrayList<>();
		for (IndexCompositeKey<I> compositeKey: compositeKeys) {
			IndividualChange<BlueKey> indexChange = IndividualChange.insert(compositeKey, destinationKey);
			indexChanges.add(indexChange);
		}
		return indexChanges;
	}

	private List<IndexCompositeKey<I>> toCompositeKeys(IndividualChange<T> change) {
		BlueKey destinationKey = change.getKey();
		T newValue = change.getNewValue();
		return toCompositeKeys(destinationKey, newValue);
	}

	private List<IndexCompositeKey<I>> toCompositeKeys(BlueKey destination, T newItem) {
		List<I> indexKeys = keyExtractor.extractKeys(newItem);
		CheckedFunction<I, IndexCompositeKey<I>> indexToComposite = (indexKey) -> new IndexCompositeKey<I>(indexKey, destination);
		List<IndexCompositeKey<I>> compositeKeys = Blutils.mapIgnoringExceptions(indexKeys, indexToComposite);
		return compositeKeys;
	}

	@Override
	public void reportReads(List<RollupTarget> rollupTargets) {
		CheckedFunction<RollupTarget, IndexRollupTarget> indexToComposite = (r) -> new IndexRollupTarget(indexName, r.getSegmentGroupingNumber(), r.getRange());
		List<IndexRollupTarget> indexRollupTargets = Blutils.mapIgnoringExceptions(rollupTargets, indexToComposite);
		collection.getRollupScheduler().reportReads(indexRollupTargets);
	}

	@Override
	public void reportWrites(List<RollupTarget> rollupTargets) {
		CheckedFunction<RollupTarget, IndexRollupTarget> indexToComposite = (r) -> new IndexRollupTarget(indexName, r.getSegmentGroupingNumber(), r.getRange());
		List<IndexRollupTarget> indexRollupTargets = Blutils.mapIgnoringExceptions(rollupTargets, indexToComposite);
		collection.getRollupScheduler().reportWrites(indexRollupTargets);
	}

	@Override
	public I getLastKey() {
		LastEntityFinder lastFinder = new LastEntityFinder(this);
		BlueEntity<?> lastIndexEntity = lastFinder.getLastEntity();
		if (lastIndexEntity == null) {
			return null;
		}
		@SuppressWarnings("unchecked")
		IndexCompositeKey<I> lastCompositeKey = (IndexCompositeKey<I>) lastIndexEntity.getKey();
		return lastCompositeKey.getIndexKey();
	}
}
