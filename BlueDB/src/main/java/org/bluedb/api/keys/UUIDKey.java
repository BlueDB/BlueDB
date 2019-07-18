package org.bluedb.api.keys;

import java.util.UUID;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.index.BlueIndex;

/**
 * A key that can be mapped to a value in a {@link BlueCollection} or {@link BlueIndex}. The hashcode of these keys
 * is used to determine the location of the corresponding values on disk. Collections with this type of key will
 * be unordered and if the hash is good then they will be spread fairly evenly across the collection segments. This 
 * means that there will be few collisions and high i-node usage. <br><br>
 */
public final class UUIDKey extends HashGroupedKey<UUID> {
	private static final long serialVersionUID = 1L;

	private final UUID id;

	public UUIDKey(UUID id) {
		this.id = id;
	}

	public UUID getId() {
		return id;
	}
}
