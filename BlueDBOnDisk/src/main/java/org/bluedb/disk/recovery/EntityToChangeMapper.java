package org.bluedb.disk.recovery;

import java.io.Serializable;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.serialization.BlueEntity;

@FunctionalInterface
public interface EntityToChangeMapper<T extends Serializable> {
	public IndividualChange<T> map(BlueEntity<T> entityToChange) throws BlueDbException;

}
