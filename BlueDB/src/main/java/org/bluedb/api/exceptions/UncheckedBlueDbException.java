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

	/**
	 * Called to read the object from a stream.
	 *
	 * @throws InvalidObjectException if the object is invalid or has a cause that is not
	 *                                an {@code BlueDbException}
	 */
	private void readObject(ObjectInputStream s)
			throws IOException, ClassNotFoundException {
		s.defaultReadObject();
		Throwable cause = super.getCause();
		if (!(cause instanceof BlueDbException)) {
			throw new InvalidObjectException("Cause must be an BlueDbException");
		}
	}

}
