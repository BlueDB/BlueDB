package org.bluedb.disk.collection;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.bluedb.api.Condition;
import org.bluedb.api.ReadBlueQuery;
import org.bluedb.api.ReadableBlueCollection;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.ReadOnlyBlueDbOnDisk;
import org.bluedb.disk.collection.index.BlueIndexOnDisk;
import org.bluedb.disk.collection.index.IndexManager;
import org.bluedb.disk.collection.metadata.CollectionMetaData;
import org.bluedb.disk.file.FileManager;
import org.bluedb.disk.query.ReadOnlyBlueQueryOnDisk;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.Segment;
import org.bluedb.disk.segment.SegmentManager;
import org.bluedb.disk.segment.SegmentSizeSetting;
import org.bluedb.disk.segment.rollup.Rollupable;
import org.bluedb.disk.serialization.BlueEntity;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;

public class ReadOnlyBlueCollectionOnDisk<T extends Serializable> implements ReadableBlueCollection<T> {

	private final Class<T> valueType;
	private final Class<? extends BlueKey> keyType;
	private final BlueSerializer serializer;
	private final Path collectionPath;
	private final FileManager fileManager;
	private final SegmentManager<T> segmentManager;
	private final CollectionMetaData metaData;
	protected final IndexManager<T> indexManager;

	public ReadOnlyBlueCollectionOnDisk(ReadOnlyBlueDbOnDisk db, String name, Class<? extends BlueKey> requestedKeyType, Class<T> valueType, List<Class<? extends Serializable>> additionalRegisteredClasses) throws BlueDbException {
		this(db, name, requestedKeyType, valueType, additionalRegisteredClasses, null);
	}

	public ReadOnlyBlueCollectionOnDisk(ReadOnlyBlueDbOnDisk db, String name, Class<? extends BlueKey> requestedKeyType, Class<T> valueType, List<Class<? extends Serializable>> additionalRegisteredClasses, SegmentSizeSetting segmentSize) throws BlueDbException {
		this.valueType = valueType;
		collectionPath = Paths.get(db.getPath().toString(), name);
		boolean isNewCollection = !collectionPath.toFile().exists();
		collectionPath.toFile().mkdirs();
		metaData = new CollectionMetaData(collectionPath);
		Class<? extends Serializable>[] classesToRegister = metaData.getAndAddToSerializedClassList(valueType, additionalRegisteredClasses);
		serializer = new ThreadLocalFstSerializer(classesToRegister);
		fileManager = new FileManager(serializer);
		segmentSize = determineSegmentSize(metaData, requestedKeyType, segmentSize, isNewCollection);
		keyType = determineKeyType(metaData, requestedKeyType);
		SegmentSizeSetting segmentSizeSettings = segmentSize;
		// TODO change SegmentManager to handle read-only well
		Rollupable rollupable = null;
		if (this instanceof Rollupable) {
			rollupable = (Rollupable) this;
		}
		segmentManager = new SegmentManager<T>(collectionPath, fileManager, rollupable, segmentSizeSettings.getConfig());
		indexManager = new IndexManager<>(this, collectionPath);
	}

	@Override
	public ReadBlueQuery<T> query() {
		return new ReadOnlyBlueQueryOnDisk<T>(this);
	}
	
	@Override
	public boolean contains(BlueKey key) throws BlueDbException {
		ensureCorrectKeyType(key);
		return get(key) != null;
	}

	@Override
	public T get(BlueKey key) throws BlueDbException {
		ensureCorrectKeyType(key);
		Segment<T> firstSegment = segmentManager.getFirstSegment(key);
		return firstSegment.get(key);
	}

	@Override
	public BlueKey getLastKey() {
		LastEntityFinder lastFinder = new LastEntityFinder(this);
		BlueEntity<?> lastEntity = lastFinder.getLastEntity();
		return lastEntity == null ? null : lastEntity.getKey();
	}

	public List<BlueEntity<T>> findMatches(Range range, List<Condition<T>> conditions, boolean byStartTime) throws BlueDbException {
		List<BlueEntity<T>> results = new ArrayList<>();
		try (CollectionEntityIterator<T> iterator = new CollectionEntityIterator<T>(segmentManager, range, byStartTime, conditions)) {
			while (iterator.hasNext()) {
				BlueEntity<T> entity = iterator.next();
				results.add(entity);
			}
		}
		return results;
	}

	public void rollup(Range timeRange) throws BlueDbException {
		Segment<T> segment = segmentManager.getSegment(timeRange.getStart());
		segment.rollup(timeRange);
	}

	public SegmentManager<T> getSegmentManager() {
		return segmentManager;
	}

	public Path getPath() {
		return collectionPath;
	}

	public FileManager getFileManager() {
		return fileManager;
	}

	public BlueSerializer getSerializer() {
		return serializer;
	}

	public CollectionMetaData getMetaData() {
		return metaData;
	}

	public Class<T> getType() {
		return valueType;
	}

	public Class<? extends BlueKey> getKeyType() {
		return keyType;
	}

	protected void ensureCorrectKeyType(BlueKey key) throws BlueDbException {
		if (!keyType.isAssignableFrom(key.getClass())) {
			throw new BlueDbException("wrong key type (" + key.getClass() + ") for Collection with key type " + keyType);
		}
	}

	protected void ensureCorrectKeyTypes(Collection<BlueKey> keys) throws BlueDbException {
		for (BlueKey key: keys) {
			ensureCorrectKeyType(key);
		}
	}

	protected static SegmentSizeSetting determineSegmentSize(CollectionMetaData metaData, Class<? extends BlueKey> keyType, SegmentSizeSetting requestedSegmentSize, boolean isNewCollection) throws BlueDbException {
		SegmentSizeSetting existingSegmentSize = metaData.getSegmentSize();
		if (existingSegmentSize == null) {
			if (!isNewCollection) {
				return SegmentSizeSetting.getOriginalDefaultSettingsFor(keyType);
			}
			existingSegmentSize = (requestedSegmentSize != null) ? requestedSegmentSize : SegmentSizeSetting.getDefaultSettingsFor(keyType);
			metaData.saveSegmentSize(existingSegmentSize);
		}
		return existingSegmentSize;
	}

	protected static Class<? extends BlueKey> determineKeyType(CollectionMetaData metaData, Class<? extends BlueKey> providedKeyType) throws BlueDbException {
		Class<? extends BlueKey> storedKeyType = metaData.getKeyType();
		if (storedKeyType == null) {
			metaData.saveKeyType(providedKeyType);
			return providedKeyType;
		} else if (providedKeyType == null) {
			return storedKeyType;
		} else if (!providedKeyType.isAssignableFrom(storedKeyType)){
			throw new BlueDbException("Cannot instantiate a Collection<" + providedKeyType + "> from a Collection<" + storedKeyType + ">");
		} else {
			return providedKeyType;
		}
	}

	//TODO: getIndex needs to work even if they haven't called initialize or build. Return empty index object if it doesn't exist
	
	@Override
	public <I extends ValueKey> BlueIndex<I, T> getIndex(String indexName, Class<I> keyType) throws BlueDbException {
		return indexManager.getIndex(indexName, keyType);
	}

	public void rollupIndex(String indexName, Range range) throws BlueDbException {
		BlueIndexOnDisk<?, T> index = indexManager.getUntypedIndex(indexName);
		index.rollup(range);
	}

	public IndexManager<T> getIndexManager() {
		return indexManager;
	}

}
