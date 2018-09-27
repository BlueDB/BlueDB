package io.bluedb.disk.serialization;

import java.io.Serializable;
import io.bluedb.api.keys.BlueKey;

public class BlueEntity<T extends Serializable> implements Serializable {
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
}
