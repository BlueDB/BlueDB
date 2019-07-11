package org.bluedb.api.index;

import java.io.Serializable;
import java.util.List;
import org.bluedb.api.keys.ValueKey;

/**
 * A function that maps a value to all the keys which should be put in an index and point to the value.
 * @param <K> the class of objects used as keys
 * @param <V> the class of objects stored in collection as values
 */
public interface KeyExtractor<K extends ValueKey, V extends Serializable> extends Serializable {
	public List<K> extractKeys(V object);
	public Class<K> getType();
}
