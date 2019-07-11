package org.bluedb.api.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.bluedb.api.keys.LongKey;

public interface LongIndexExtractor<V extends Serializable> extends KeyExtractor<LongKey, V> {
	
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

	public List<Long> extractLongsForIndex(V value);
}
