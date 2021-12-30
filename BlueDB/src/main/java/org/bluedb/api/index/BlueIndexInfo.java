package org.bluedb.api.index;

import java.io.Serializable;

import org.bluedb.api.keys.ValueKey;

public class BlueIndexInfo<K extends ValueKey, V extends Serializable> {
	private String name;
	private Class<K> keyType;
	private KeyExtractor<K, V> keyExtractor;
	
	public BlueIndexInfo(String name, Class<K> keyType, KeyExtractor<K, V> keyExtractor) {
		this.name = name;
		this.keyType = keyType;
		this.keyExtractor = keyExtractor;
	}
	
	public String getName() {
		return name;
	}
	
	public Class<K> getKeyType() {
		return keyType;
	}
	
	public KeyExtractor<K, V> getKeyExtractor() {
		return keyExtractor;
	}
}
