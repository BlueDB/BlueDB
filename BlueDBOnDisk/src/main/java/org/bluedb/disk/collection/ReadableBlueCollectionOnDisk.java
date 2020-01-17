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
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.ReadableBlueDbOnDisk;
import org.bluedb.disk.collection.index.ReadableBlueIndexOnDisk;
import org.bluedb.disk.collection.metadata.ReadWriteCollectionMetaData;
import org.bluedb.disk.collection.metadata.ReadableCollectionMetadata;
import org.bluedb.disk.file.ReadFileManager;
import org.bluedb.disk.query.ReadOnlyBlueQueryOnDisk;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadableSegment;
import org.bluedb.disk.segment.ReadableSegmentManager;
import org.bluedb.disk.segment.SegmentSizeSetting;
import org.bluedb.disk.serialization.BlueEntity;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;

public abstract class ReadableBlueCollectionOnDisk<T extends Serializable> implements ReadableBlueCollection<T> {

	private final Class<T> valueType;
	private final Class<? extends BlueKey> keyType;
	protected final BlueSerializer serializer;
	protected final Path collectionPath;
	protected final SegmentSizeSetting segmentSizeSettings;

	protected abstract ReadableCollectionMetadata getOrCreateMetadata();
	protected abstract Class<? extends Serializable>[] getClassesToRegister(Class<? extends BlueKey> requestedKeyType, List<Class<? extends Serializable>> additionalRegisteredClasses) throws BlueDbException;
	public abstract ReadFileManager getFileManager();
	public abstract ReadableSegmentManager<T> getSegmentManager();
	public abstract <I extends ValueKey> ReadableBlueIndexOnDisk<I, T> getIndex(String indexName, Class<I> keyType) throws BlueDbException;

	public ReadableBlueCollectionOnDisk(ReadableBlueDbOnDisk db, String name, Class<? extends BlueKey> requestedKeyType, Class<T> valueType, List<Class<? extends Serializable>> additionalRegisteredClasses, SegmentSizeSetting segmentSize) throws BlueDbException {
		this.valueType = valueType;
		collectionPath = Paths.get(db.getPath().toString(), name);
		boolean isNewCollection = !collectionPath.toFile().exists();
		collectionPath.toFile().mkdirs();
		ReadableCollectionMetadata metaData = getOrCreateMetadata();
		Class<? extends Serializable>[] classesToRegister = getClassesToRegister(requestedKeyType, additionalRegisteredClasses);
		serializer = new ThreadLocalFstSerializer(classesToRegister);
		keyType = determineKeyType(metaData, requestedKeyType);
		segmentSizeSettings = determineSegmentSize(metaData, requestedKeyType, segmentSize, isNewCollection);
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
		ReadableSegment<T> firstSegment = getSegmentManager().getFirstSegment(key);
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
		try (CollectionEntityIterator<T> iterator = new CollectionEntityIterator<T>(getSegmentManager(), range, byStartTime, conditions)) {
			while (iterator.hasNext()) {
				BlueEntity<T> entity = iterator.next();
				results.add(entity);
			}
		}
		return results;
	}

	public Path getPath() {
		return collectionPath;
	}

	public BlueSerializer getSerializer() {
		return serializer;
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

	protected static SegmentSizeSetting determineSegmentSize(ReadableCollectionMetadata metaData, Class<? extends BlueKey> keyType, SegmentSizeSetting requestedSegmentSize, boolean isNewCollection) throws BlueDbException {
		SegmentSizeSetting existingSegmentSize = metaData.getSegmentSize();
		if (existingSegmentSize == null) {
			if (!isNewCollection) {
				return SegmentSizeSetting.getOriginalDefaultSettingsFor(keyType);
			}
			existingSegmentSize = (requestedSegmentSize != null) ? requestedSegmentSize : SegmentSizeSetting.getDefaultSettingsFor(keyType);
			if (metaData instanceof ReadWriteCollectionMetaData) {
				((ReadWriteCollectionMetaData)metaData).saveSegmentSize(existingSegmentSize);
			}
		}
		return existingSegmentSize;
	}

	protected static Class<? extends BlueKey> determineKeyType(ReadableCollectionMetadata metaData, Class<? extends BlueKey> providedKeyType) throws BlueDbException {
		Class<? extends BlueKey> storedKeyType = metaData.getKeyType();
		if (storedKeyType == null) {
			if (metaData instanceof ReadWriteCollectionMetaData) {
				((ReadWriteCollectionMetaData)metaData).saveKeyType(providedKeyType);
			}
			return providedKeyType;
		} else if (providedKeyType == null) {
			return storedKeyType;
		} else if (!providedKeyType.isAssignableFrom(storedKeyType)){
			throw new BlueDbException("Cannot instantiate a Collection<" + providedKeyType + "> from a Collection<" + storedKeyType + ">");
		} else {
			return providedKeyType;
		}
	}

}
