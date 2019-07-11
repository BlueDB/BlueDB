package org.bluedb.api.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.bluedb.api.keys.IntegerKey;

public interface IntegerIndexExtractor<V extends Serializable> extends KeyExtractor<IntegerKey, V> {
	
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

	public List<Integer> extractIntsForIndex(V value);
}
