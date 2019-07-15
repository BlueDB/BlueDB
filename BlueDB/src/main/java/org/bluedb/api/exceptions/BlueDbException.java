package org.bluedb.api.exceptions;

/**
 * Exception in BlueDb
 */
public class BlueDbException extends Exception {
	private static final long serialVersionUID = 1L;

	/**
	 * Exception in BlueDb.
	 * @param message - a message describing what happened
	 * @param cause - the underlying exception
	 */
	public BlueDbException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Exception in BlueDb.
	 * @param message - a message describing what happened
	 */
	public BlueDbException(String message) {
		super(message);
	}
}
