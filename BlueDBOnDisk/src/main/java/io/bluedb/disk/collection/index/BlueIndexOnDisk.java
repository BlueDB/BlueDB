package io.bluedb.disk.collection.index;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import io.bluedb.api.BlueIndex;
import io.bluedb.api.KeyExtractor;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.collection.CollectionEntityIterator;
import io.bluedb.disk.file.FileManager;
import io.bluedb.disk.segment.Range;
import io.bluedb.disk.segment.Segment;
import io.bluedb.disk.segment.SegmentManager;
import io.bluedb.disk.segment.rollup.RollupScheduler;
import io.bluedb.disk.segment.rollup.RollupTarget;
import io.bluedb.disk.segment.rollup.Rollupable;

public class BlueIndexOnDisk<K extends BlueKey, T extends Serializable> implements BlueIndex<K, T>, Rollupable {

	private final static String FILE_KEY_EXTRACTOR = ".extractor";
	private final static String FILE_ = ".extractor";

	private final BlueCollectionOnDisk<T> collection;
	private final KeyExtractor<K, T> keyExtractor;
//	private final BlueSerializer serializer;
	private final Path indexPath;
	private final FileManager fileManager;
	private final SegmentManager<BlueKey> segmentManager;
	private final RollupScheduler rollupScheduler;
//	private final Class<? extends BlueKey> keyType;

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
		KeyExtractor<K, T> keyExtractor = (KeyExtractor<K, T>) fileManager.loadObject(keyExtractorPath);
		return new BlueIndexOnDisk<K, T>(collection, indexPath, keyExtractor);
	}

	private BlueIndexOnDisk(BlueCollectionOnDisk<T> collection, Path indexPath, KeyExtractor<K, T> keyExtractor) {
		this.collection = collection;
		this.keyExtractor = keyExtractor;
		this.fileManager = collection.getFileManager();
		rollupScheduler = new RollupScheduler(this);
		this.indexPath = indexPath;
		segmentManager = new SegmentManager<BlueKey>(indexPath, fileManager, rollupScheduler, keyExtractor.getType());
	}

	//	@Override
//	public BlueQuery<T> query() {
//		return new IndexQuery<K, T>(collection, this);
//	}

	@Override
	public void add(BlueKey key, T newItem) throws BlueDbException {
		if (newItem == null) {
			return;
		}
		IndexKey<K> indexKey = toIndexKey(key, newItem);
		Segment<BlueKey> segment = segmentManager.getFirstSegment(indexKey);
		segment.insert(indexKey, key);
	}

	@Override
	public void remove(K key, T oldItem) throws BlueDbException {
		if (oldItem == null) {
			return;
		}
		IndexKey<K> indexKey = toIndexKey(key, oldItem);
		Segment<BlueKey> segment = segmentManager.getFirstSegment(indexKey);
		segment.delete(indexKey);
	}

	@Override
	public List<T> get(K key) throws BlueDbException {
//		Segment<BlueKey> segment = segmentManager.getFirstSegment(key);
//		Range range = new Range(key.getGroupingNumber(), key.getGroupingNumber());
//		List<BlueEntity<T>> entities = collection.findMatches(range, new ArrayList<>(), true);
//		List<IndexKey<K>> indexKeys = Blutils.map(entities, (e) -> (IndexKey<K>)e.getKey());
//		List<BlueKey> underlyingKeys = Blutils.map(indexKeys, (k) -> k.getTargetKey());
		List<BlueKey> underlyingKeys = getKeys(key);
		List<T> values = Blutils.map(underlyingKeys, (k) -> collection.get(k));
		return values;
	}

	public List<BlueKey> getKeys(K key) {
		Range range = new Range(key.getGroupingNumber(), key.getGroupingNumber());
		List<BlueKey> keys = new ArrayList<>();
		try (CollectionEntityIterator<BlueKey> entityIterator = new CollectionEntityIterator<>(segmentManager, range, true)) {
			while (entityIterator.hasNext()) {
				IndexKey<K> indexKey = (IndexKey<K>) entityIterator.next().getKey();
				keys.add(indexKey.getTargetKey());
			}
		}
		return keys;
	}

//	public BlueCollectionOnDisk<T> getCollection() {
//		return collection;
//	}

	@Override
	public void scheduleRollup(RollupTarget target) {
		// TODO Auto-generated method stub
		
	}

//	public SegmentManager<BlueKey> getSegmentManager() {
//		return segmentManager;
//	}

	public void shutdown() {
		rollupScheduler.forceScheduleRollups();
		rollupScheduler.stop();
	}

	private IndexKey<K> toIndexKey(BlueKey key, T newItem) {
		return new IndexKey<K>(keyExtractor.extractKey(newItem), key);
	}
}
