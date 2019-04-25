package org.bluedb.api.exceptions;

import org.bluedb.api.keys.BlueKey;

public class DuplicateKeyException extends BlueDbException {
	private static final long serialVersionUID = 1L;
	private final BlueKey key;

	public DuplicateKeyException(String message, BlueKey key) {
		super(message);
		this.key = key;
	}

	public BlueKey getKey() {
		return key;
	}
}
