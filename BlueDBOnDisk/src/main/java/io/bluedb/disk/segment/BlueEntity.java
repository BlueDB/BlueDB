package io.bluedb.disk.segment;

import java.io.Serializable;
import io.bluedb.api.keys.BlueKey;

public class BlueEntity<T extends Serializable> implements Serializable {
	private static final long serialVersionUID = 1L;

	private BlueKey key;
	private T object;

	public BlueEntity(BlueKey key, T object) {
		this.key = key;
		this.object = object;
	}

	public BlueKey getKey() {
		return key;
	}

	public T getObject() {
		return object;
	}
}
