package org.bluedb.api.index;

import java.io.Serializable;
import java.util.List;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;

/**
 * Index that maps a key to a set of values indexedto that key.
 * @param <K> the class of the keys
 * @param <T> the class of objects stored in collection as values
 */
public interface BlueIndex<K extends BlueKey, T extends Serializable> {

	/**
	 * @param key key that maps to the desired value
	 * @return all values that are indexed the the key
	 * @throws BlueDbException if any problems occur
	 */
	public List<T> get(K key) throws BlueDbException;

	/**
	 * @return the key with the highest grouping number
	 */
	public K getLastKey();
}
