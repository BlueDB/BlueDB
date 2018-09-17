package io.bluedb.disk.collection.index;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.index.BlueIndex;
import io.bluedb.api.index.KeyExtractor;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.Blutils.CheckedFunction;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.collection.CollectionEntityIterator;
import io.bluedb.disk.file.FileManager;
import io.bluedb.disk.segment.Range;
import io.bluedb.disk.segment.Segment;
import io.bluedb.disk.segment.SegmentManager;
import io.bluedb.disk.segment.rollup.IndexRollupTarget;
import io.bluedb.disk.segment.rollup.RollupTarget;
import io.bluedb.disk.segment.rollup.Rollupable;

public class BlueIndexOnDisk<K extends BlueKey, T extends Serializable> implements BlueIndex<K, T>, Rollupable {

	private final static String FILE_KEY_EXTRACTOR = ".extractor";

	private final BlueCollectionOnDisk<T> collection;
	private final KeyExtractor<K, T> keyExtractor;
	private final FileManager fileManager;
	private final SegmentManager<BlueKey> segmentManager;
	private final String indexName;

	public static <K extends BlueKey, T extends Serializable> BlueIndexOnDisk<K, T> createNew(BlueCollectionOnDisk<T> collection, Path indexPath, KeyExtractor<K, T> keyExtractor) throws BlueDbException {
		indexPath.toFile().mkdirs();
		FileManager fileManager = collection.getFileManager();
		Path keyExtractorPath = Paths.get(indexPath.toString(), FILE_KEY_EXTRACTOR);
		fileManager.saveObject(keyExtractorPath, keyExtractor);
		return new BlueIndexOnDisk<K, T>(collection, indexPath, keyExtractor);
	}

	public static <K extends BlueKey, T extends Serializable> BlueIndexOnDisk<K, T> fromExisting(BlueCollectionOnDisk<T> collection, Path indexPath) throws BlueDbException {
		FileManager fileManager = collection.getFileManager();
		Path keyExtractorPath = Paths.get(indexPath.toString(), FILE_KEY_EXTRACTOR);
		@SuppressWarnings("unchecked")
		KeyExtractor<K, T> keyExtractor = (KeyExtractor<K, T>) fileManager.loadObject(keyExtractorPath);
		return new BlueIndexOnDisk<K, T>(collection, indexPath, keyExtractor);
	}

	public Class<K> getType() {
		return keyExtractor.getType();
	}

	private BlueIndexOnDisk(BlueCollectionOnDisk<T> collection, Path indexPath, KeyExtractor<K, T> keyExtractor) {
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
		for (IndexCompositeKey<K> compositeKey: toCompositeKeys(key, newItem)) {
			Segment<BlueKey> segment = segmentManager.getFirstSegment(compositeKey);
			segment.insert(compositeKey, key);
		}
	}

	public void remove(K key, T oldItem) throws BlueDbException {
		if (oldItem == null) {
			return;
		}
		for (IndexCompositeKey<K> compositeKey: toCompositeKeys(key, oldItem)) {
			Segment<BlueKey> segment = segmentManager.getFirstSegment(compositeKey);
			segment.delete(compositeKey);
		}
	}

	@Override
	public List<T> get(K key) throws BlueDbException {
		List<BlueKey> underlyingKeys = getKeys(key);
		List<T> values = Blutils.map(underlyingKeys, (k) -> collection.get(k));
		return values;
	}

	public List<BlueKey> getKeys(K key) {
		Range range = new Range(key.getGroupingNumber(), key.getGroupingNumber());
		List<BlueKey> keys = new ArrayList<>();
		try (CollectionEntityIterator<BlueKey> entityIterator = new CollectionEntityIterator<>(segmentManager, range, true, new ArrayList<>())) {
			while (entityIterator.hasNext()) {
				@SuppressWarnings("unchecked")
				IndexCompositeKey<K> indexKey = (IndexCompositeKey<K>) entityIterator.next().getKey();
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

	private List<IndexCompositeKey<K>> toCompositeKeys(BlueKey destination, T newItem) throws BlueDbException {
		List<K> indexKeys = keyExtractor.extractKeys(newItem);
		CheckedFunction<K, IndexCompositeKey<K>> indexToComposite = (indexKey) -> new IndexCompositeKey<K>(indexKey, destination);
		List<IndexCompositeKey<K>> compositeKeys = Blutils.map(indexKeys, indexToComposite);
		return compositeKeys;
	}

	@Override
	public void reportRead(long segmentGroupingNumber, Range range) {
		IndexRollupTarget target = new IndexRollupTarget(indexName, segmentGroupingNumber, range);
		collection.getRollupScheduler().reportRead(target);
	}

	@Override
	public void reportWrite(long segmentGroupingNumber, Range range) {
		RollupTarget target = new RollupTarget(segmentGroupingNumber, range);
		collection.getRollupScheduler().reportWrite(target);
	}
}
