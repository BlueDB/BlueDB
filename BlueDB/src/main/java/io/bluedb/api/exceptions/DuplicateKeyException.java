package io.bluedb.api.exceptions;

public class DuplicateKeyException extends BlueDbException {
	private static final long serialVersionUID = 1L;

	public DuplicateKeyException(String message) {
		super(message);
	}
}
