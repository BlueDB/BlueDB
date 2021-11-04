package org.bluedb.disk.recovery;

import java.io.Serializable;
import java.util.Map.Entry;
import java.util.Objects;

import org.bluedb.api.Mapper;
import org.bluedb.api.Updater;
import org.bluedb.api.datastructures.BlueKeyValuePair;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.file.ComparableAndSerializable;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.serialization.BlueEntity;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.validation.SerializationException;

public class IndividualChange <T extends Serializable> implements ComparableAndSerializable<IndividualChange<T>> {

	private static final long serialVersionUID = 1L;

	private final BlueKey key;
	private final T oldValue;
	private final T newValue;

	public static <T extends Serializable> IndividualChange<T> createInsertChange(BlueKeyValuePair<T> keyValuePair) {
		return createInsertChange(keyValuePair.getKey(), keyValuePair.getValue());
	}

	public static <T extends Serializable> IndividualChange<T> createInsertChange(Entry<BlueKey, T> entry) {
		return createInsertChange(entry.getKey(), entry.getValue());
	}

	public static <T extends Serializable> IndividualChange<T> createInsertChange(BlueKey key, T value) {
		return new IndividualChange<T>(key, null, value);
	}

	public static <T extends Serializable> IndividualChange<T> createDeleteChange(BlueEntity<T> entity) {
		return createDeleteChange(entity.getKey(), entity.getValue());
	}
	
	public static <T extends Serializable> IndividualChange<T> createDeleteChange(BlueKey key, T value) {
		return new IndividualChange<T>(key, value, null);
	}

	public static <T extends Serializable> IndividualChange<T> createUpdateChange(BlueEntity<T> entity, Updater<T> updater, BlueSerializer serializer) throws SerializationException {
		return createUpdateChange(entity.getKey(), entity.getValue(), updater, serializer);
	}

	public static <T extends Serializable> IndividualChange<T> createUpdateChange(BlueKey key, T value, Updater<T> updater, BlueSerializer serializer) throws SerializationException {
		T oldValue = serializer.clone(value);
		T newValue = serializer.clone(oldValue);
		updater.update(newValue);
		return new IndividualChange<T>(key, oldValue, newValue);
	}

	public static <T extends Serializable> IndividualChange<T> createReplaceChange(BlueEntity<T> entity, Mapper<T> mapper, BlueSerializer serializer) throws SerializationException {
		return createReplaceChange(entity.getKey(), entity.getValue(), mapper, serializer);
	}

	public static <T extends Serializable> IndividualChange<T> createReplaceChange(BlueKey key, T value, Mapper<T> mapper, BlueSerializer serializer) throws SerializationException {
		T oldValue = serializer.clone(value);
		T newValue = mapper.update(serializer.clone(oldValue));
		return new IndividualChange<T>(key, oldValue, newValue);
	}

	public static <T extends Serializable> IndividualChange<T> createChange(BlueKey key, T oldValue, T newValue) {
		return new IndividualChange<T>(key, oldValue, newValue);
	}

	public IndividualChange(BlueKey key, T oldValue, T newValue) {
		this.key = key;
		this.oldValue = oldValue;
		this.newValue = newValue;
	}

	public BlueKey getKey() {
		return key;
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
		return getKey().isInRange(range.getStart(), range.getEnd());
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
					Objects.equals(oldValue, other.oldValue) &&
					Objects.equals(newValue, other.newValue);
        }
        
        return false;
	}

	@Override
	public String toString() {
		return "IndividualChange [key=" + key + ", oldValue=" + oldValue + ", newValue=" + newValue + "]";
	}
	
}
