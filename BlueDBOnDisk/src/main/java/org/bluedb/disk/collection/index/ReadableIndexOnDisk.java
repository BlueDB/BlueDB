package org.bluedb.disk.collection.index;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.bluedb.api.CloseableIterator;
import org.bluedb.api.Condition;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.exceptions.UnsupportedIndexConditionTypeException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.index.conditions.IntegerIndexCondition;
import org.bluedb.api.index.conditions.LongIndexCondition;
import org.bluedb.api.index.conditions.StringIndexCondition;
import org.bluedb.api.index.conditions.UUIDIndexCondition;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.LongTimeKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.UUIDKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.collection.CollectionEntityIterator;
import org.bluedb.disk.collection.LastEntityFinder;
import org.bluedb.disk.collection.ReadableCollectionOnDisk;
import org.bluedb.disk.collection.index.conditions.IncludedSegmentRangeInfo;
import org.bluedb.disk.collection.index.conditions.OnDiskIntegerIndexCondition;
import org.bluedb.disk.collection.index.conditions.OnDiskLongIndexCondition;
import org.bluedb.disk.collection.index.conditions.OnDiskStringIndexCondition;
import org.bluedb.disk.collection.index.conditions.OnDiskUUIDIndexCondition;
import org.bluedb.disk.collection.index.extractors.DefaultTimeKeyExtractor;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadableSegmentManager;
import org.bluedb.disk.segment.SegmentSizeSetting;
import org.bluedb.disk.serialization.BlueEntity;

public abstract class ReadableIndexOnDisk<I extends ValueKey, T extends Serializable> implements BlueIndex<I, T> {

	protected final static String FILE_KEY_EXTRACTOR = ".extractor";

	private final ReadableCollectionOnDisk<T> collection;
	protected final Path indexPath;
	private final KeyExtractor<I, T> keyExtractor;

	public abstract ReadableSegmentManager<BlueKey> getSegmentManager();
	
	public ReadableSegmentManager<T> getSegmentManagerForIndexedCollection() {
		return collection.getSegmentManager();
	}

	public Class<I> getType() {
		return keyExtractor.getType();
	}
	
	public Class<T> getCollectionType() {
		return collection.getType();
	}

	public String getName() {
		return indexPath.toFile().getName();
	}
	
	public Path getIndexPath() {
		return indexPath;
	}

	protected ReadableIndexOnDisk(ReadableCollectionOnDisk<T> collection, Path indexPath, KeyExtractor<I, T> keyExtractor) throws BlueDbException {
		this.collection = collection;
		this.indexPath = indexPath;
		this.keyExtractor = keyExtractor;
	}

	protected static SegmentSizeSetting determineSegmentSize(Class<? extends BlueKey> keyType) throws BlueDbException {
		/*
		 * TODO: This will change to use the default index setting once we support different index segment sizes. We
		 * may require information from the meta data in order to make this determiniation. We may also need to
		 * know if the index is new and if the user has requeste a particular size for this index. See BlueCollectionOnDisk#determineSegmentSize
		 * for inspiration.
		 */
		return SegmentSizeSetting.getOriginalDefaultSettingsFor(keyType);
	}

	public List<I> extractIndexKeys(BlueEntity<T> entity) {
		return extractIndexKeys(entity.getKey(), entity.getValue());
	}

	public List<I> extractIndexKeys(BlueKey key, T value) {
		List<I> extractedKeys;
		if(keyExtractor instanceof DefaultTimeKeyExtractor) {
			extractedKeys = extractKeysForDefaultTimeIndex(key);
		} else {
			extractedKeys = keyExtractor.extractKeys(value);
		}
		
		return extractedKeys != null ? extractedKeys : new LinkedList<>();
	}
	
	private List<I> extractKeysForDefaultTimeIndex(BlueKey key) {
		return ((DefaultTimeKeyExtractor<I, T>)keyExtractor).extractKeys(key, getSegmentManagerForIndexedCollection());
	}

	/**
	 * Returns the keys that contain the exact given indexed data. This should only be used in tests to verify that
	 * the index data is correct after making changes.
	 * @param targetIndexKey the indexed data that should be contained in values in order for their keys to be returned
	 * @return a set of keys for values that contain the target indexed data
	 */
	public Set<BlueKey> getKeys(I targetIndexKey) {
		Range range = new Range(targetIndexKey.getGroupingNumber(), targetIndexKey.getGroupingNumber());
		List<Condition<BlueKey>> valueKeyConditions = new LinkedList<>();
		List<Condition<BlueKey>> indexKeyConditions = new LinkedList<>();
		indexKeyConditions.add(key -> {
			return targetIndexKey.equals(((IndexCompositeKey<?>)key).getIndexKey());	
		});
		Optional<IncludedSegmentRangeInfo> includedSegmentRangeInfo = Optional.empty();
		
		Set<BlueKey> keys = new HashSet<>();
		try (CloseableIterator<BlueEntity<BlueKey>> entityIterator = getEntities(range, indexKeyConditions, valueKeyConditions, includedSegmentRangeInfo)) {
			while (entityIterator.hasNext()) {
				@SuppressWarnings("unchecked")
				IndexCompositeKey<I> indexKey = (IndexCompositeKey<I>) entityIterator.next().getKey();
				keys.add(indexKey.getValueKey());
			}
		}
		return keys;
	}

	public CloseableIterator<BlueEntity<BlueKey>> getEntities(Range range, List<Condition<BlueKey>> indexKeyConditions, List<Condition<BlueKey>> valueKeyConditions, Optional<IncludedSegmentRangeInfo> includedIndexSegmentRangeInfo) {
		return new CollectionEntityIterator<BlueKey>(getSegmentManager(), range, true, new LinkedList<>(), valueKeyConditions, indexKeyConditions, includedIndexSegmentRangeInfo);
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
	
	@Override
	public IntegerIndexCondition createIntegerIndexCondition() throws UnsupportedIndexConditionTypeException {
		if(IntegerKey.class.isAssignableFrom(keyExtractor.getType())) {
			return new OnDiskIntegerIndexCondition<T>(this);
		}
		throw new UnsupportedIndexConditionTypeException("IntegerIndexCondition is unsupported for an index with a key type of " + keyExtractor.getType().getCanonicalName());
	}
	
	@Override
	public LongIndexCondition createLongIndexCondition() throws UnsupportedIndexConditionTypeException {
		if(LongKey.class.isAssignableFrom(keyExtractor.getType()) || LongTimeKey.class.isAssignableFrom(keyExtractor.getType())) {
			return new OnDiskLongIndexCondition<T>(this);
		}
		throw new UnsupportedIndexConditionTypeException("LongIndexCondition is unsupported for an index with a key type of " + keyExtractor.getType().getCanonicalName());
	}
	
	@Override
	public StringIndexCondition createStringIndexCondition() throws UnsupportedIndexConditionTypeException {
		if(StringKey.class.isAssignableFrom(keyExtractor.getType())) {
			return new OnDiskStringIndexCondition<T>(this);
		}
		throw new UnsupportedIndexConditionTypeException("StringIndexCondition is unsupported for an index with a key type of " + keyExtractor.getType().getCanonicalName());
	}
	
	@Override
	public UUIDIndexCondition createUUIDIndexCondition() throws UnsupportedIndexConditionTypeException {
		if(UUIDKey.class.isAssignableFrom(keyExtractor.getType())) {
			return new OnDiskUUIDIndexCondition<T>(this);
		}
		throw new UnsupportedIndexConditionTypeException("UUIDIndexCondition is unsupported for an index with a key type of " + keyExtractor.getType().getCanonicalName());
	}

	public Range getIndexSegmentRangeForIndexKey(ValueKey key) {
		ReadableSegmentManager<BlueKey> segmentManager = getSegmentManager();
		return segmentManager.toRange(segmentManager.getPathManager().getSegmentPath(key));
	}

	public Range getCollectionSegmentRangeForValueKey(BlueKey valueKey) {
		ReadableSegmentManager<T> collectionSegmentManager = getSegmentManagerForIndexedCollection();
		return collectionSegmentManager.toRange(collectionSegmentManager.getPathManager().getSegmentPath(valueKey));
	}
}
