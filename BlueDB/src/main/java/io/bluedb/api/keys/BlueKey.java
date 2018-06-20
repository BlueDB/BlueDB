package io.bluedb.api.keys;

import java.io.Serializable;

public interface BlueKey extends Serializable, Comparable<BlueKey> {
	@Override
	public abstract int hashCode();

	@Override
	public abstract boolean equals(Object object);

	@Override
	public abstract int compareTo(BlueKey other);
	
	public long getGroupingNumber();
}
