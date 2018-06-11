package io.bluedb.api.exceptions;

public class BlueDbException extends Exception {
	private static final long serialVersionUID = 1L;

	public BlueDbException() {
		super();
	}

	public BlueDbException(String message, Throwable cause) {
		super(message, cause);
	}

	public BlueDbException(String message) {
		super(message);
	}

	public BlueDbException(Throwable cause) {
		super(cause);
	}
}
