package io.bluedb.api;

import java.io.Serializable;
import io.bluedb.api.keys.BlueKey;

public interface KeyExtractor<K extends BlueKey, T extends Serializable> extends Serializable {
	public K extractKey(T object);
	public Class<K> getType();
}
