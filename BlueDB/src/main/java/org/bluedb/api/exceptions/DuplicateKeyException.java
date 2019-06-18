package org.bluedb.api.exceptions;

import org.bluedb.api.keys.BlueKey;

/**
 * Exception caused by inserting a value to a key that already has a value.
 */
public class DuplicateKeyException extends BlueDbException {
	private static final long serialVersionUID = 1L;
	private final BlueKey key;

	/**
	 * exception caused by inserting a value to a key that already has a value
	 * @param message message describing what happened
	 * @param key key that already has a value associated with it
	 */
	public DuplicateKeyException(String message, BlueKey key) {
		super(message);
		this.key = key;
	}

	/**
	 * get key already associated to a value
	 * @return key that already has a value associated with it
	 */
	public BlueKey getKey() {
		return key;
	}
}
