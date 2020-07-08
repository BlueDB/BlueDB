package org.bluedb.disk.recovery;

import java.io.Serializable;
import java.util.Objects;

import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.serialization.BlueEntity;

public class IndividualChange <T extends Serializable> implements Serializable, Comparable<IndividualChange<T>> {

	private static final long serialVersionUID = 1L;

	private final BlueKey key;
	private final T oldValue;
	private final T newValue;

	public static <T extends Serializable> IndividualChange<T> createInsertChange(BlueKey key, T value) {
		return new IndividualChange<T>(key, null, value);
	}

	public static <T extends Serializable> IndividualChange<T> createDeleteChange(BlueKey key) {
		return new IndividualChange<T>(key, null, null);
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
