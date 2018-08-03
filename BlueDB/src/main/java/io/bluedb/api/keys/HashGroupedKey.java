package io.bluedb.api.keys;

@SuppressWarnings("serial")
public abstract class HashGroupedKey extends ValueKey {
	@Override
	public final long getGroupingNumber() {
		long hashCodeAsLong = hashCode();
		long integerMinAsLong = Integer.MIN_VALUE;
		return hashCodeAsLong + Math.abs(integerMinAsLong);
	}
}
