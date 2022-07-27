package org.bluedb.api.datastructures;

import java.io.Serializable;

import org.bluedb.api.keys.TimeKey;

public class TimeKeyValuePair<V extends Serializable> {
	private TimeKey timeKey;
	private V value;
	
	public TimeKeyValuePair(TimeKey key, V value) {
		this.timeKey = key;
		this.value = value;
	}
	
	public TimeKey getKey() {
		return timeKey;
	}
	
	public V getValue() {
		return value;
	}
}
