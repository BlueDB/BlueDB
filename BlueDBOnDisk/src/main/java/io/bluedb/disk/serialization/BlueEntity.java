package io.bluedb.disk.serialization;

import java.io.Serializable;
import java.util.Objects;

import io.bluedb.api.keys.BlueKey;

public final class BlueEntity<T extends Serializable> implements Serializable, Comparable<BlueEntity<T>> {
	private static final long serialVersionUID = 1L;

	private BlueKey key;
	private T value;

	public BlueEntity(BlueKey key, T value) {
		this.key = key;
		this.value = value;
	}

	public BlueKey getKey() {
		return key;
	}

	public T getValue() {
		return value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof BlueEntity))
			return false;
		BlueEntity<?> other = (BlueEntity<?>) obj;
		return Objects.equals(key,  other.key) && Objects.equals(value,  other.value);
	}

	@Override
	public int compareTo(BlueEntity<T> other) {
		if (key == null && other.key == null) {
			return 0;
		} else if (key == null) {
			return -1;
		} else if (other.key == null) {
			return 1;
		} else {
			return key.compareTo(other.key);
		}
	}

	@Override
	public String toString() {
		return "BlueEntity [" + key + ": " + value + "]";
	}
}
