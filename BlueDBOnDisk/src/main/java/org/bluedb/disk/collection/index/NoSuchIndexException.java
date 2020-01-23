package org.bluedb.disk.collection.index;

import org.bluedb.api.exceptions.BlueDbException;

public class NoSuchIndexException extends BlueDbException {

	private static final long serialVersionUID = 1L;

	public NoSuchIndexException(String message) {
		super(message);
	}
}
