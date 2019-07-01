package org.bluedb.api.index;

import java.io.Serializable;
import java.util.List;
import org.bluedb.api.keys.ValueKey;

public interface KeyExtractor<K extends ValueKey, V extends Serializable> extends Serializable {
	public List<K> extractKeys(V object);
	public Class<K> getType();
}
