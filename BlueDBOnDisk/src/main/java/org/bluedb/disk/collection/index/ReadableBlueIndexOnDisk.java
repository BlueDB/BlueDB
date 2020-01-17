package org.bluedb.disk.collection.index;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.collection.CollectionEntityIterator;
import org.bluedb.disk.collection.LastEntityFinder;
import org.bluedb.disk.collection.ReadableBlueCollectionOnDisk;
import org.bluedb.disk.file.ReadFileManager;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadableSegmentManager;
import org.bluedb.disk.segment.SegmentSizeSetting;
import org.bluedb.disk.serialization.BlueEntity;

public abstract class ReadableBlueIndexOnDisk<I extends ValueKey, T extends Serializable> implements BlueIndex<I, T> {

	protected final static String FILE_KEY_EXTRACTOR = ".extractor";

	private final ReadableBlueCollectionOnDisk<T> collection;
	protected final KeyExtractor<I, T> keyExtractor;

	public abstract ReadableSegmentManager<BlueKey> getSegmentManager();
	public abstract ReadFileManager getFileManager();

	public Class<I> getType() {
		return keyExtractor.getType();
	}

	protected ReadableBlueIndexOnDisk(ReadableBlueCollectionOnDisk<T> collection, Path indexPath, KeyExtractor<I, T> keyExtractor) throws BlueDbException {
		this.collection = collection;
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
		List<BlueKey> underlyingKeys = getKeys(key);
		List<T> values = Blutils.map(underlyingKeys, (k) -> collection.get(k));
		return values;
	}

	public List<BlueKey> getKeys(I key) {
		Range range = new Range(key.getGroupingNumber(), key.getGroupingNumber());
		List<BlueKey> keys = new ArrayList<>();
		try (CollectionEntityIterator<BlueKey> entityIterator = new CollectionEntityIterator<BlueKey>(getSegmentManager(), range, true, new ArrayList<>())) {
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
