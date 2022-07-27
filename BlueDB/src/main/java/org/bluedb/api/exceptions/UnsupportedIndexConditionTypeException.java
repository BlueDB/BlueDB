package org.bluedb.api.exceptions;

/**
 * Exception caused by attempting to create an index condition of the wrong type.
 */
public class UnsupportedIndexConditionTypeException extends BlueDbException {
	private static final long serialVersionUID = 1L;

	/**
	 * Exception caused by attempting to create an index condition of the wrong type.
	 * @param message a message describing what happened
	 */
	public UnsupportedIndexConditionTypeException(String message) {
		super(message);
	}
}
