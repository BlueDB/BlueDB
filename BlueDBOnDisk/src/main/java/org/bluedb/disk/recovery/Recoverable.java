package org.bluedb.disk.recovery;

import java.io.Serializable;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.collection.ReadOnlyBlueCollectionOnDisk;

public interface Recoverable<T extends Serializable> extends Comparable<Recoverable<?>> {
	public void apply(ReadOnlyBlueCollectionOnDisk<T> collection) throws BlueDbException;
	public long getTimeCreated();
	public long getRecoverableId();
	public void setRecoverableId(long recoverableId);

	@Override
	public default int compareTo(Recoverable<?> other) {
		if (getTimeCreated() == other.getTimeCreated()) {
			return Long.compare(getRecoverableId(), other.getRecoverableId());
		} else {
			return Long.compare(getTimeCreated(), other.getTimeCreated());
		}
	}
}
