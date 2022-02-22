package org.bluedb.disk.recovery;

import java.io.Serializable;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;

@FunctionalInterface
public interface KeyValueToChangeMapper<T extends Serializable> {
	public IndividualChange<T> map(BlueKey key, T value) throws BlueDbException;
}
