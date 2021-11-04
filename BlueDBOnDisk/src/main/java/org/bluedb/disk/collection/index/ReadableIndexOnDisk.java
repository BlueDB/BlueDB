package org.bluedb.disk.collection.index;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.bluedb.api.Condition;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.collection.CollectionEntityIterator;
import org.bluedb.disk.collection.LastEntityFinder;
import org.bluedb.disk.collection.ReadableCollectionOnDisk;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadableSegmentManager;
import org.bluedb.disk.segment.SegmentSizeSetting;
import org.bluedb.disk.serialization.BlueEntity;

public abstract class ReadableIndexOnDisk<I extends ValueKey, T extends Serializable> implements BlueIndex<I, T> {

	protected final static String FILE_KEY_EXTRACTOR = ".extractor";

	private final ReadableCollectionOnDisk<T> collection;
	protected final Path indexPath;
	protected final KeyExtractor<I, T> keyExtractor;

	public abstract ReadableSegmentManager<BlueKey> getSegmentManager();

	public Class<I> getType() {
		return keyExtractor.getType();
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

	@Override
	public List<T> get(I key) throws BlueDbException {
		Set<BlueKey> underlyingKeys = getKeys(key);
		return collection.query()
			.whereKeyIsIn(underlyingKeys)
			.where(value -> {
				return valueContainsIndexKey(value, key);
			})
			.getList();
	}
	
	private boolean valueContainsIndexKey(T value, I indexKey) {
		List<I> indexKeys = keyExtractor.extractKeys(value);
		return indexKeys != null && indexKeys.contains(indexKey);	
	}
	
	public Set<BlueKey> getKeys(I targetIndexKey) {
		Range range = new Range(targetIndexKey.getGroupingNumber(), targetIndexKey.getGroupingNumber());
		List<Condition<BlueKey>> conditions = new LinkedList<>();
		List<Condition<BlueKey>> indexKeyConditions = new LinkedList<>();
		indexKeyConditions.add(key -> {
			return targetIndexKey.equals(((IndexCompositeKey<?>)key).getIndexKey());	
		});
		Optional<Set<Range>> segmentRangesToInclude = Optional.empty();
		
		Set<BlueKey> keys = new HashSet<>();
		try (CollectionEntityIterator<BlueKey> entityIterator = new CollectionEntityIterator<BlueKey>(getSegmentManager(), range, true, conditions, indexKeyConditions, segmentRangesToInclude)) {
			while (entityIterator.hasNext()) {
				@SuppressWarnings("unchecked")
				IndexCompositeKey<I> indexKey = (IndexCompositeKey<I>) entityIterator.next().getKey();
				keys.add(indexKey.getValueKey());
			}
		}
		return keys;
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
