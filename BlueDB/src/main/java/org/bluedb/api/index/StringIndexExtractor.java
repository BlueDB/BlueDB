package org.bluedb.api.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.bluedb.api.keys.StringKey;

public interface StringIndexExtractor<V extends Serializable> extends KeyExtractor<StringKey, V> {
	
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

	public List<String> extractStringsForIndex(V value);
}
