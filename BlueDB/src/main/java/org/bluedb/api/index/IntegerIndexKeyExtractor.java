package org.bluedb.api.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.keys.IntegerKey;

/**
 * This is used by the {@link BlueIndex} class to extract integers from each value in the {@link BlueCollection}. 
 * The value can then be quickly found using those integers. Note that key extractors are serialized by {@link BlueIndex} 
 * and for this reason it is <b>NOT</b> recommended to use lambdas or anonymous 
 * inner classes to define key extractors.
 * 
 * @param <V> - the value type of the collection being indexed
 */
public interface IntegerIndexKeyExtractor<V extends Serializable> extends KeyExtractor<IntegerKey, V> {
	
	public default Class<IntegerKey> getType() {
		return IntegerKey.class;
	}

	public default List<IntegerKey> extractKeys(V value) {
		List<Integer> ints = extractIntsForIndex(value);
		if(ints != null) {
			return ints.stream()
				.filter(Objects::nonNull)
				.map(i -> new IntegerKey(i))
				.collect(Collectors.toCollection(ArrayList::new));
		}
		return new ArrayList<>();
	}

	/**
	 * Extracts integers from the given value in order to create index keys. The {@link BlueIndex} class
	 * uses this method to map the resulting index keys to this value. 
	 * @param value - the value from which index keys are to be extracted
	 * @return one or many integers that this value should be indexed on
	 */
	public List<Integer> extractIntsForIndex(V value);
}
