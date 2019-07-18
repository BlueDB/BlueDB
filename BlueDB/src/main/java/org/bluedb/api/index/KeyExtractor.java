package org.bluedb.api.index;

import java.io.Serializable;
import java.util.List;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.UUIDKey;
import org.bluedb.api.keys.ValueKey;

/**
 * This is used by the {@link BlueIndex} class to extract data in the form of index keys from each value in 
 * the {@link BlueCollection}. The value can then be quickly found using those index keys.
 * 
 * <br><br>
 * The following sub types are more simple and can be used in place of this: {@link IntegerIndexKeyExtractor}, 
 * {@link LongIndexKeyExtractor}, {@link StringIndexKeyExtractor}, {@link UUIDIndexKeyExtractor}. Note that key extractors
 * are serialized by {@link BlueIndex} and for this reason it is <b>NOT</b> recommended to use lambdas or anonymous 
 * inner classes to define key extractors.
 * 
 * @param <K> the key type of the index or the type of data that the collection is being indexed on. It must be a concretion of 
 * {@link ValueKey} ({@link UUIDKey}, {@link StringKey}, {@link LongKey}, or {@link IntegerKey}).
 * @param <V> the value type of the collection being indexed
 */
public interface KeyExtractor<K extends ValueKey, V extends Serializable> extends Serializable {
	/**
	 * Extracts data from the given value in the form of index keys. The {@link BlueIndex} class
	 * uses this method to map the resulting index keys to this value. 
	 * @param value the value from which index keys are to be extracted
	 * @return one or many index keys representing the data that this value should be indexed on
	 */
	public List<K> extractKeys(V value);
	
	/**
	 * @return the index key type class
	 */
	public Class<K> getType();
}
