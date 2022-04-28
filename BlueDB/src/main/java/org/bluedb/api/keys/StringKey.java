package org.bluedb.api.keys;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.index.BlueIndex;

/**
 * A key that can be mapped to a value in a {@link BlueCollection} or {@link BlueIndex}. The hashcode of these keys
 * is used to determine the location of the corresponding values on disk. Collections with this type of key will
 * be unordered and if the hash is good then they will be spread fairly evenly across the collection segments. This 
 * means that there will be few collisions and high i-node usage.
 */
public final class StringKey extends HashGroupedKey<String> {
	private static final long serialVersionUID = 1L;

	private final String id;

	public StringKey(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}
	
	@Override
	public String getStringIdIfPresent() {
		return id;
	}
}
