package org.bluedb.api.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.keys.UUIDKey;

/**
 * This is used by the {@link BlueIndex} class to extract uuids from each value in the {@link BlueCollection}. 
 * The value can then be quickly found using those uuids. Note that key extractors are serialized by {@link BlueIndex} 
 * and for this reason it is <b>NOT</b> recommended to use lambdas or anonymous inner classes to define key extractors.
 * 
 * @param <V> - the value type of the collection being indexed
 */
public interface UUIDIndexKeyExtractor<V extends Serializable> extends KeyExtractor<UUIDKey, V> {
	
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
	
	/**
	 * Extracts uuids from the given value in order to create index keys. The {@link BlueIndex} class
	 * uses this method to map the resulting index keys to this value. 
	 * @param value - the value from which index keys are to be extracted
	 * @return one or many uuids that this value should be indexed on
	 */
	public List<UUID> extractUUIDsForIndex(V value);
}
