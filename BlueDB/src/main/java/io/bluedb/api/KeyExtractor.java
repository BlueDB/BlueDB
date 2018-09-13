package io.bluedb.api;

import java.io.Serializable;
import io.bluedb.api.keys.ValueKey;

public interface KeyExtractor<K extends ValueKey, T extends Serializable> extends Serializable {
	public K extractKey(T object);
	public Class<K> getType();
}
