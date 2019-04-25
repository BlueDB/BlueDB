package org.bluedb.api.keys;

import java.util.UUID;

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
