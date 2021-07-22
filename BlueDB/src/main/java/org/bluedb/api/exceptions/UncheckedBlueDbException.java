package org.bluedb.api.exceptions;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.util.Objects;

/**
 * Wraps a {@link BlueDbException} with an unchecked exception.
 */
public class UncheckedBlueDbException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Constructs an instance of this class.
	 *
	 * @param message the detail message, can be null
	 * @param cause   the {@code BlueDbException}
	 * @throws NullPointerException if the cause is {@code null}
	 */
	public UncheckedBlueDbException(String message, BlueDbException cause) {
		super(message, Objects.requireNonNull(cause));
	}

	/**
	 * Constructs an instance of this class.
	 *
	 * @param cause the {@code BlueDbException}
	 * @throws NullPointerException if the cause is {@code null}
	 */
	public UncheckedBlueDbException(BlueDbException cause) {
		super(Objects.requireNonNull(cause));
	}

	/**
	 * Returns the cause of this exception.
	 *
	 * @return the {@code BlueDbException} which is the cause of this exception.
	 */
	@Override
	public BlueDbException getCause() {
		return (BlueDbException) super.getCause();
	}

}
