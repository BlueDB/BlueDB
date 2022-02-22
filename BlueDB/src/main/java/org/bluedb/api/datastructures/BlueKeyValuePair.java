package org.bluedb.api.datastructures;

import java.io.Serializable;

import org.bluedb.api.keys.BlueKey;

public class BlueKeyValuePair<V extends Serializable> {
	private BlueKey key;
	private V value;
	
	public BlueKeyValuePair(BlueKey key, V value) {
		this.key = key;
		this.value = value;
	}
	
	public BlueKey getKey() {
		return key;
	}
	
	public V getValue() {
		return value;
	}
}
