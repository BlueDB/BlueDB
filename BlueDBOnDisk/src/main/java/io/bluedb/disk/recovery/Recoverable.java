package io.bluedb.disk.recovery;

import java.io.Serializable;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.collection.BlueCollectionOnDisk;

public interface Recoverable<T extends Serializable> {
	public void apply(BlueCollectionOnDisk<T> collection) throws BlueDbException;
	public long getTimeCreated();
}
