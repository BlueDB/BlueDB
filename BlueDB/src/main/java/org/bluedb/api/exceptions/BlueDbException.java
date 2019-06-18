package org.bluedb.api.exceptions;

/**
 * Exception in BlueDb
 */
public class BlueDbException extends Exception {
	private static final long serialVersionUID = 1L;

	/**
	 * Exception for BlueDb.
	 * @param message message describing what happened
	 * @param cause underlying exception
	 */
	public BlueDbException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Exception for BlueDb.
	 * @param message message describing what happened
	 */
	public BlueDbException(String message) {
		super(message);
	}
}
