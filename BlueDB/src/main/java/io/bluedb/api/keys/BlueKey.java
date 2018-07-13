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
	
	default public Long getLongIdIfPresent() {
		return null;
	}

	default public Integer getIntegerIdIfPresent() {
		return null;
	}

	default boolean isInRange(long min, long max) {
		return getGroupingNumber() >= min && getGroupingNumber() <= max;
	}
}
