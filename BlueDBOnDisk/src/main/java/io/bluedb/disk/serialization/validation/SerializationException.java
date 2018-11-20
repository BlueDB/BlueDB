package io.bluedb.disk.serialization.validation;

import io.bluedb.api.exceptions.BlueDbException;

public class SerializationException extends BlueDbException {
	private static final long serialVersionUID = 1L;

	public SerializationException(String message) {
		super(message);
	}

	public SerializationException(String message, Throwable cause) {
		super(message, cause);
	}
}
