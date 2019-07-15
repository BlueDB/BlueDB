package org.bluedb.api.index;

import java.io.Serializable;
import java.util.List;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.UUIDKey;
import org.bluedb.api.keys.ValueKey;

/**
 * An index on a {@link BlueCollection} that allows you to query for values faster based on specific data in those values.
 * BlueDB extracts data from each value in the collection and creates a mapping from that data to the values that contain that
 * data. Currently this only supports the retrieval of values using a specific index key, but could be extended in the future to
 * provide more powerful index based queries.
 * 
 * @param <K> - the key type of the index or the type of data that the collection is being indexed on. It must be a concretion of 
 * {@link ValueKey} ({@link UUIDKey}, {@link StringKey}, {@link LongKey}, or {@link IntegerKey}).
 * @param <V> - the value type of the collection being indexed
 */
public interface BlueIndex<K extends ValueKey, V extends Serializable> {

	/**
	 * @param key - a key that maps to the desired value(s)
	 * @return all the values in the collection for the given index key
	 * @throws BlueDbException if any problems occur
	 */
	public List<V> get(K key) throws BlueDbException;

	/**
	 * @return the index key with the highest grouping number
	 */
	public K getLastKey();
}
