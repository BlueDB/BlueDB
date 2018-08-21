package io.bluedb.api;

import java.io.Serializable;
import io.bluedb.api.keys.BlueKey;

@FunctionalInterface
public interface KeyExtractor<K extends BlueKey, T extends Serializable> {
	public K extractKey(T object);
}
