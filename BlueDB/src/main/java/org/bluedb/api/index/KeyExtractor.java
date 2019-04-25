package org.bluedb.api.index;

import java.io.Serializable;
import java.util.List;
import org.bluedb.api.keys.ValueKey;

public interface KeyExtractor<K extends ValueKey, T extends Serializable> extends Serializable {
	public List<K> extractKeys(T object);
	public Class<K> getType();
}
