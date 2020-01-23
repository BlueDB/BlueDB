package org.bluedb.disk.collection;

import org.bluedb.api.exceptions.BlueDbException;

public class NoSuchCollectionException extends BlueDbException {

	private static final long serialVersionUID = 1L;

	public NoSuchCollectionException(String message) {
		super(message);
	}
}
