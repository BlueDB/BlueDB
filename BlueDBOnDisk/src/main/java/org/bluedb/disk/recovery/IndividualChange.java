package org.bluedb.disk.recovery;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

import org.bluedb.api.Mapper;
import org.bluedb.api.TimeEntityMapper;
import org.bluedb.api.TimeEntityUpdater;
import org.bluedb.api.Updater;
import org.bluedb.api.datastructures.BlueKeyValuePair;
import org.bluedb.api.datastructures.TimeKeyValuePair;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.file.ComparableAndSerializable;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.serialization.BlueEntity;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.validation.SerializationException;
import org.nustaq.serialization.annotations.Version;

public class IndividualChange <T extends Serializable> implements ComparableAndSerializable<IndividualChange<T>> {

	private static final long serialVersionUID = 1L;

	private final BlueKey key;
	private final T oldValue;
	private final T newValue;
	
	@Version(1) private final BlueKey originalKeyIfDifferent;

	public static <T extends Serializable> IndividualChange<T> createInsertChange(BlueKeyValuePair<T> keyValuePair) {
		return createInsertChange(keyValuePair.getKey(), keyValuePair.getValue());
	}

	public static <T extends Serializable> IndividualChange<T> createInsertChange(BlueKey key, T value) {
		return new IndividualChange<T>(key, null, value, Optional.empty());
	}

	public static <T extends Serializable> IndividualChange<T> createDeleteChange(BlueEntity<T> entity) {
		return createDeleteChange(entity.getKey(), entity.getValue());
	}
	
	public static <T extends Serializable> IndividualChange<T> createDeleteChange(BlueKey key, T value) {
		return new IndividualChange<T>(key, value, null, Optional.empty());
	}

	public static <T extends Serializable> IndividualChange<T> createUpdateChange(BlueEntity<T> entity, Updater<T> updater, BlueSerializer serializer) throws SerializationException {
		return createUpdateChange(entity.getKey(), entity.getValue(), updater, serializer);
	}

	public static <T extends Serializable> IndividualChange<T> createUpdateChange(BlueKey key, T value, Updater<T> updater, BlueSerializer serializer) throws SerializationException {
		T oldValue = serializer.clone(value);
		T newValue = serializer.clone(oldValue);
		updater.update(newValue);
		
		return new IndividualChange<T>(key, oldValue, newValue, Optional.empty());
	}

	public static <T extends Serializable> IndividualChange<T> createUpdateChange(BlueEntity<T> oldEntity, T newValue) throws BlueDbException {
		return new IndividualChange<T>(oldEntity.getKey(), oldEntity.getValue(), newValue, Optional.empty());
	}

	public static <T extends Serializable> IndividualChange<T> createUpdateKeyAndValueChange(BlueKey originalKey, T originalValue, TimeEntityUpdater<T> updater, BlueSerializer serializer) throws BlueDbException {
		T oldValue = serializer.clone(originalValue);
		T newValue = serializer.clone(oldValue);
		TimeKey newKey = updater.update(newValue);
		
		if(!Objects.equals(originalKey, newKey)) {
			throw new BlueDbException("Invalid new key created in update query. If you are going to change the key of a record then the new key must be equivalent. A TimeKey, ActiveTimeKey, or TimeFrameKey would all be considered equivalent if the time and id are the same. The end time on a TimeFrameKey isn't significant for equivalence.");
		}
		
		Optional<BlueKey> originalKeyIfDifferent = getOriginalKeyIfChanged(originalKey, newKey);
		
		return new IndividualChange<T>(newKey, oldValue, newValue, originalKeyIfDifferent);
	}

	public static <T extends Serializable> IndividualChange<T> createUpdateKeyAndValueChange(BlueEntity<T> oldEntity, BlueEntity<T> newEntity) throws BlueDbException {
		if(!Objects.equals(oldEntity.getKey(), newEntity.getKey())) {
			throw new BlueDbException("Invalid new key created in update query. If you are going to change the key of a record then the new key must be equivalent. A TimeKey, ActiveTimeKey, or TimeFrameKey would all be considered equivalent if the time and id are the same. The end time on a TimeFrameKey isn't significant for equivalence.");
		}
		
		Optional<BlueKey> originalKeyIfDifferent = getOriginalKeyIfChanged(oldEntity.getKey(), newEntity.getKey());
		
		return new IndividualChange<T>(newEntity.getKey(), oldEntity.getValue(), newEntity.getValue(), originalKeyIfDifferent);
	}

	private static Optional<BlueKey> getOriginalKeyIfChanged(BlueKey oldKey, BlueKey newKey) {
		if(oldKey.getClass() != newKey.getClass()) {
			return Optional.of(oldKey);
		}
		
		if(oldKey instanceof TimeFrameKey && newKey instanceof TimeFrameKey) {
			if(((TimeFrameKey)oldKey).getEndTime() != ((TimeFrameKey)newKey).getEndTime()) {
				return Optional.of(oldKey);
			}
		}
		
		return Optional.empty();
	}

	public static <T extends Serializable> IndividualChange<T> createReplaceChange(BlueEntity<T> entity, Mapper<T> mapper, BlueSerializer serializer) throws SerializationException {
		return createReplaceChange(entity.getKey(), entity.getValue(), mapper, serializer);
	}

	public static <T extends Serializable> IndividualChange<T> createReplaceChange(BlueKey key, T value, Mapper<T> mapper, BlueSerializer serializer) throws SerializationException {
		T oldValue = serializer.clone(value);
		T newValue = mapper.update(serializer.clone(oldValue));
		return new IndividualChange<T>(key, oldValue, newValue, Optional.empty());
	}

	public static <T extends Serializable> IndividualChange<T> createReplaceKeyAndValueChange(BlueKey originalKey, T originalValue, TimeEntityMapper<T> mapper, BlueSerializer serializer) throws BlueDbException {
		T oldValue = (T) serializer.clone(originalValue);
		
		TimeKeyValuePair<T> newEntity = mapper.map(serializer.clone(oldValue));
		if(!Objects.equals(originalKey, newEntity.getKey())) {
			throw new BlueDbException("Invalid new key created in replace query. If you are going to change the key of a record then the new key must be equivalent. A TimeKey, ActiveTimeKey, or TimeFrameKey would all be considered equivalent if the time and id are the same. The end time on a TimeFrameKey isn't significant for equivalence.");
		}
		
		Optional<BlueKey> originalKeyIfDifferent = getOriginalKeyIfChanged(originalKey, newEntity.getKey());
		
		return new IndividualChange<T>(newEntity.getKey(), oldValue, newEntity.getValue(), originalKeyIfDifferent);
	}

	public static <T extends Serializable> IndividualChange<T> manuallyCreateTestChange(BlueKey key, T oldValue, T newValue, Optional<BlueKey> originalKeyIfDifferent) {
		return new IndividualChange<T>(key, oldValue, newValue, originalKeyIfDifferent);
	}

	private IndividualChange(BlueKey key, T oldValue, T newValue, Optional<BlueKey> originalKeyIfDifferent) {
		this.key = key;
		this.oldValue = oldValue;
		this.newValue = newValue;
		this.originalKeyIfDifferent = originalKeyIfDifferent.orElse(null);
	}

	public BlueKey getKey() {
		return key;
	}
	
	public boolean isKeyChanged() {
		return originalKeyIfDifferent != null;
	}
	
	public BlueKey getOriginalKey() {
		if(originalKeyIfDifferent != null) {
			return originalKeyIfDifferent;
		} else {
			return key;
		}
	}
	
	public T getOldValue() {
		return oldValue;
	}

	public T getNewValue() {
		return newValue;
	}
	
	public BlueEntity<T> getNewEntity() {
		if (newValue == null) {
			return null;
		} else {
			return new BlueEntity<T>(key, newValue);
		}
	}

	public long getGroupingNumber() {
		return getKey().getGroupingNumber();
	}

	public boolean overlaps(Range range) {
		return getKey().overlapsRange(range.getStart(), range.getEnd());
	}

	@Override
	public int compareTo(IndividualChange<T> otherChange) {
		return getKey().compareTo(otherChange.getKey());
	}

	@Override
	public int hashCode() {
		return Objects.hash(key, oldValue, newValue);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
            return true;
		}
		
        if (obj instanceof IndividualChange) {
			IndividualChange<?> other = (IndividualChange<?>) obj;
			return Objects.equals(key, other.key) &&
					Objects.equals(originalKeyIfDifferent, other.originalKeyIfDifferent) &&
					Objects.equals(oldValue, other.oldValue) &&
					Objects.equals(newValue, other.newValue);
        }
        
        return false;
	}

	@Override
	public String toString() {
		return "IndividualChange [key=" + key + ", oldValue=" + oldValue + ", newValue=" + newValue
				+ ", originalKeyIfDifferent=" + originalKeyIfDifferent + "]";
	}
	
}
