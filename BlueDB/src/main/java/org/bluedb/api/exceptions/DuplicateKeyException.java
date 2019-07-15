package org.bluedb.api.exceptions;

import org.bluedb.api.keys.BlueKey;

/**
 * Exception caused by inserting a value for a key that already has a value associated with it
 */
public class DuplicateKeyException extends BlueDbException {
	private static final long serialVersionUID = 1L;
	private final BlueKey key;

	/**
	 * Exception caused by inserting a value for a key that already has a value associated with it
	 * @param message - a message describing what happened
	 * @param key - the key that already has a value associated with it
	 */
	public DuplicateKeyException(String message, BlueKey key) {
		super(message);
		this.key = key;
	}

	/**
	 * Get the key that already has a value associated with it
	 * @return the key that already has a value associated with it
	 */
	public BlueKey getKey() {
		return key;
	}
}
