package io.bluedb.disk;

import java.io.Serializable;
import io.bluedb.api.keys.BlueKey;

public class BlueEntity implements Serializable {
	private static final long serialVersionUID = 1L;

	private BlueKey key;
	private Serializable object;

	public BlueEntity(BlueKey key, Serializable object) {
		this.key = key;
		this.object = object;
	}

	public BlueKey getKey() {
		return key;
	}

	public Serializable getObject() {
		return object;
	}
}
