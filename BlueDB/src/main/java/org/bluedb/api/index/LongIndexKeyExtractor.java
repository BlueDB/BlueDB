package org.bluedb.api.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.keys.LongKey;

/**
 * This is used by the {@link BlueIndex} class to extract longs from each value in the {@link BlueCollection}. 
 * The value can then be quickly found using those longs. Note that key extractors are serialized by {@link BlueIndex} 
 * and for this reason it is <b>NOT</b> recommended to use lambdas or anonymous inner classes to define key extractors.
 * 
 * @param <V> - the value type of the collection being indexed
 */
public interface LongIndexKeyExtractor<V extends Serializable> extends KeyExtractor<LongKey, V> {
	
	public default Class<LongKey> getType() {
		return LongKey.class;
	}

	public default List<LongKey> extractKeys(V value) {
		List<Long> longs = extractLongsForIndex(value);
		if(longs != null) {
			return longs.stream()
				.filter(Objects::nonNull)
				.map(l -> new LongKey(l))
				.collect(Collectors.toCollection(ArrayList::new));
		}
		return new ArrayList<>();
	}

	/**
	 * Extracts longs from the given value in order to create index keys. The {@link BlueIndex} class
	 * uses this method to map the resulting index keys to this value. 
	 * @param value - the value from which index keys are to be extracted
	 * @return one or many longs that this value should be indexed on
	 */
	public List<Long> extractLongsForIndex(V value);
}
