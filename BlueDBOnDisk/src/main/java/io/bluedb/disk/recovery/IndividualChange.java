package io.bluedb.disk.recovery;

import java.io.Serializable;

import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.serialization.BlueEntity;

public class IndividualChange <T extends Serializable> implements Serializable, Comparable<IndividualChange<T>> {

	private static final long serialVersionUID = 1L;

	private final BlueKey key;
	private final T oldValue;
	private final T newValue;

	public static <T extends Serializable> IndividualChange<T> insert(BlueKey key, T value) {
		return new IndividualChange<T>(key, null, value);
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

	@Override
	public int compareTo(IndividualChange<T> otherChange) {
		return getKey().compareTo(otherChange.getKey());
	}
}
