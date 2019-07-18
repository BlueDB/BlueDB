package org.bluedb.api.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.keys.StringKey;

/**
 * This is used by the {@link BlueIndex} class to extract strings from each value in the {@link BlueCollection}. 
 * The value can then be quickly found using those strings. Note that key extractors are serialized by {@link BlueIndex} 
 * and for this reason it is <b>NOT</b> recommended to use lambdas or anonymous inner classes to define key extractors.
 * 
 * @param <V> the value type of the collection being indexed
 */
public interface StringIndexKeyExtractor<V extends Serializable> extends KeyExtractor<StringKey, V> {
	
	public default Class<StringKey> getType() {
		return StringKey.class;
	}

	public default List<StringKey> extractKeys(V value) {
		List<String> strings = extractStringsForIndex(value);
		if(strings != null) {
			return strings.stream()
				.filter(Objects::nonNull)
				.map(s -> new StringKey(s))
				.collect(Collectors.toCollection(ArrayList::new));
		}
		return new ArrayList<>();
	}

	/**
	 * Extracts strings from the given value in order to create index keys. The {@link BlueIndex} class
	 * uses this method to map the resulting index keys to this value. 
	 * @param value the value from which index keys are to be extracted
	 * @return one or many strings that this value should be indexed on
	 */
	public List<String> extractStringsForIndex(V value);
}
