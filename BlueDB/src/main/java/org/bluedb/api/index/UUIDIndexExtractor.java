package org.bluedb.api.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import org.bluedb.api.keys.UUIDKey;

public interface UUIDIndexExtractor<V extends Serializable> extends KeyExtractor<UUIDKey, V> {
	
	public default Class<UUIDKey> getType() {
		return UUIDKey.class;
	}
	
	public default List<UUIDKey> extractKeys(V value) {
		List<UUID> uuids = extractUUIDsForIndex(value);
		if(uuids != null) {
			return uuids.stream()
				.filter(Objects::nonNull)
				.map(id -> new UUIDKey(id))
				.collect(Collectors.toCollection(ArrayList::new));
		}
		return new ArrayList<>();
	}
	
	public List<UUID> extractUUIDsForIndex(V value);
}
