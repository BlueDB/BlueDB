package org.bluedb.disk.collection;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.bluedb.api.BlueCollectionVersion;
import org.bluedb.api.Condition;
import org.bluedb.api.ReadBlueQuery;
import org.bluedb.api.ReadableBlueCollection;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.conditions.BlueIndexCondition;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.LongTimeKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.ReadableDbOnDisk;
import org.bluedb.disk.collection.index.ReadableIndexOnDisk;
import org.bluedb.disk.collection.index.conditions.IncludedSegmentRangeInfo;
import org.bluedb.disk.collection.index.conditions.OnDiskIndexCondition;
import org.bluedb.disk.collection.index.conditions.dummy.OnDiskDummyIndexCondition;
import org.bluedb.disk.collection.metadata.ReadWriteCollectionMetaData;
import org.bluedb.disk.collection.metadata.ReadableCollectionMetadata;
import org.bluedb.disk.config.ConfigurationService;
import org.bluedb.disk.encryption.EncryptionServiceWrapper;
import org.bluedb.disk.file.ReadFileManager;
import org.bluedb.disk.query.QueryIndexConditionGroup;
import org.bluedb.disk.query.ReadOnlyQueryOnDisk;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadableSegment;
import org.bluedb.disk.segment.ReadableSegmentManager;
import org.bluedb.disk.segment.SegmentSizeSetting;
import org.bluedb.disk.serialization.BlueEntity;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;

public abstract class ReadableCollectionOnDisk<T extends Serializable> implements ReadableBlueCollection<T> {
	
	public static final String OVERLAPPING_TIME_SEGMENTS_INDEX_NAME = "overlapping-time-segments-index";
	public static final String ACTIVE_RECORD_TIMES_INDEX_NAME = "active-record-times-index";

	private final Class<T> valueType;
	private final Class<? extends BlueKey> keyType;
	protected final ConfigurationService configurationService;
	protected final EncryptionServiceWrapper encryptionService;
	protected final BlueSerializer serializer;
	protected final Path collectionPath;
	protected final SegmentSizeSetting segmentSizeSettings;
	protected final BlueCollectionVersion version;
	private final boolean utilizesDefaultTimeIndex;

	protected abstract ReadableCollectionMetadata getOrCreateMetadata();
	protected abstract Class<? extends Serializable>[] getClassesToRegister(List<Class<? extends Serializable>> additionalRegisteredClasses) throws BlueDbException;
	public abstract ReadFileManager getFileManager();
	public abstract ReadableSegmentManager<T> getSegmentManager();
	public abstract <I extends ValueKey> BlueIndex<I, T> getIndex(String indexName, Class<I> keyType) throws BlueDbException;

	public ReadableCollectionOnDisk(ReadableDbOnDisk db, String name, BlueCollectionVersion requestedVersion, Class<? extends BlueKey> requestedKeyType, Class<T> valueType, List<Class<? extends Serializable>> additionalRegisteredClasses, SegmentSizeSetting segmentSize) throws BlueDbException {
		this.valueType = valueType;
		collectionPath = Paths.get(db.getPath().toString(), name);
		boolean isNewCollection = !collectionPath.toFile().exists();
		collectionPath.toFile().mkdirs();
		configurationService = db.getConfigurationService();
		encryptionService = db.getEncryptionService();
		ReadableCollectionMetadata metaData = getOrCreateMetadata();
		Class<? extends Serializable>[] classesToRegister = getClassesToRegister(additionalRegisteredClasses);
		serializer = new ThreadLocalFstSerializer(db.getConfigurationService(), classesToRegister);
		keyType = determineKeyType(metaData, requestedKeyType);
		segmentSizeSettings = determineSegmentSize(metaData, keyType, segmentSize, isNewCollection);
		version = determineCollectionVersion(metaData, requestedVersion, isNewCollection);
		this.utilizesDefaultTimeIndex = isTimeBased() && version.utilizesDefaultTimeIndex();
	}

	@Override
	public ReadBlueQuery<T> query() {
		return new ReadOnlyQueryOnDisk<T>(this);
	}
	
	@Override
	public boolean contains(BlueKey key) throws BlueDbException {
		ensureCorrectKeyType(key);
		return get(key) != null;
	}

	@Override
	public T get(BlueKey key) throws BlueDbException {
		BlueEntity<T> entity = getEntity(key);
		return entity != null ? entity.getValue() : null;
	}

	public BlueEntity<T> getEntity(BlueKey key) throws BlueDbException {
		ensureCorrectKeyType(key);
		ReadableSegment<T> firstSegment = getSegmentManager().getFirstSegment(key);
		return firstSegment.getEntity(key);
	}

	@Override
	public BlueKey getLastKey() {
		LastEntityFinder lastFinder = new LastEntityFinder(this);
		BlueEntity<?> lastEntity = lastFinder.getLastEntity();
		return lastEntity == null ? null : lastEntity.getKey();
	}

	@Override
	@SuppressWarnings("unchecked")
	public T getLastValue() {
		LastEntityFinder lastFinder = new LastEntityFinder(this);
		BlueEntity<T> lastEntity = (BlueEntity<T>) lastFinder.getLastEntity();
		return lastEntity == null ? null : lastEntity.getValue();
	}

	public List<BlueEntity<T>> findMatches(Range range, List<QueryIndexConditionGroup<T>> indexConditionGroups, List<Condition<T>> conditions, List<Condition<BlueKey>> keyConditions, boolean byStartTime, Optional<IncludedSegmentRangeInfo> includedSegmentRangeInfo) throws BlueDbException {
		List<BlueEntity<T>> results = new ArrayList<>();
		try (CollectionEntityIterator<T> iterator = new CollectionEntityIterator<T>(getSegmentManager(), range, byStartTime, indexConditionGroups, conditions, keyConditions, includedSegmentRangeInfo)) {
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
	
	public SegmentSizeSetting getSegmentSizeSettings() {
		return segmentSizeSettings;
	}
	
	public BlueCollectionVersion getVersion() {
		return version;
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
	
	public boolean isTimeBased() {
		return TimeKey.class.isAssignableFrom(getKeyType());
	}
	
	public void ensureCorrectKeyType(BlueKey key) throws BlueDbException {
		if (!keyType.isAssignableFrom(key.getClass())) {
			throw new BlueDbException("wrong key type (" + key.getClass() + ") for Collection with key type " + keyType);
		}
	}
	
	public boolean utilizesDefaultTimeIndex() {
		return utilizesDefaultTimeIndex;
	}
	
	public BlueIndex<LongTimeKey, T> getOverlappingTimeSegmentsIndex() throws BlueDbException {
		return getIndex(OVERLAPPING_TIME_SEGMENTS_INDEX_NAME, LongTimeKey.class);
	}
	
	public BlueIndex<LongTimeKey, T> getActiveRecordTimesIndex() throws BlueDbException {
		return getIndex(ACTIVE_RECORD_TIMES_INDEX_NAME, LongTimeKey.class);
	}
	
	protected static SegmentSizeSetting determineSegmentSize(ReadableCollectionMetadata metaData, Class<? extends BlueKey> keyType, SegmentSizeSetting requestedSegmentSize, boolean isNewCollection) throws BlueDbException {
		SegmentSizeSetting segmentSize = metaData.getSegmentSize();
		if (segmentSize == null) {
			if (!isNewCollection) {
				segmentSize = SegmentSizeSetting.getOriginalDefaultSettingsFor(keyType);
			} else {
				segmentSize = (requestedSegmentSize != null) ? requestedSegmentSize : SegmentSizeSetting.getDefaultSettingsFor(keyType);
			}
			
			if (metaData instanceof ReadWriteCollectionMetaData) {
				((ReadWriteCollectionMetaData)metaData).saveSegmentSize(segmentSize);
			}
		}
		return segmentSize;
	}

	protected static BlueCollectionVersion determineCollectionVersion(ReadableCollectionMetadata metaData, BlueCollectionVersion requestedCollectionVersion, boolean isNewCollection) throws BlueDbException {
		BlueCollectionVersion collectionVersion = metaData.getCollectionVersion();
		if (collectionVersion == null) {
			if (!isNewCollection) {
				collectionVersion = BlueCollectionVersion.VERSION_1; //The version of all legacy collections that don't have version meta data
			} else {
				collectionVersion = (requestedCollectionVersion != null) ? requestedCollectionVersion : BlueCollectionVersion.getDefault();
			}
			
			if (metaData instanceof ReadWriteCollectionMetaData) {
				((ReadWriteCollectionMetaData)metaData).saveCollectionVersion(collectionVersion);
			}
		}
		return collectionVersion;
	}

	protected static Class<? extends BlueKey> determineKeyType(ReadableCollectionMetadata metaData, Class<? extends BlueKey> providedKeyType) throws BlueDbException {
		Class<? extends BlueKey> storedKeyType = metaData.getKeyType();
		
		if(storedKeyType == null && providedKeyType == null) {
			throw new BlueDbException("Unable to determine key type for collection at " + metaData.getPath() + ". You must initialize a pre-existing valid collection or provide a key type.");
		} 
		
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
	
	//Full integration tests to verify this. Also for facade collections/indices
	public boolean isCompatibleIndexCondition(BlueIndexCondition<?> indexCondition) {
		if(indexCondition instanceof OnDiskIndexCondition) {
			OnDiskIndexCondition<?, ?> onDiskIndexCondition = (OnDiskIndexCondition<?, ?>) indexCondition;
			
			if(!valueType.isAssignableFrom(onDiskIndexCondition.getIndexedCollectionType())) {
				return false;
			}
			
			if(onDiskIndexCondition instanceof OnDiskDummyIndexCondition) {
				return true;
			}
			
			try {
				BlueIndex<? extends ValueKey, T> actualIndex = getIndex(onDiskIndexCondition.getIndexName(), onDiskIndexCondition.getIndexKeyType());
				if(actualIndex instanceof ReadableIndexOnDisk) {
					return Objects.equals(onDiskIndexCondition.getIndexPath(), ((ReadableIndexOnDisk<?,?>)actualIndex).getIndexPath());
				}
			} catch (BlueDbException e) {
				return false;
			}
		}
		return false;
	}

}
