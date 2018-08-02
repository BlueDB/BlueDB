package io.bluedb.api.keys;

@SuppressWarnings("serial")
public abstract class ValueKey implements BlueKey {
	@Override
	public long getGroupingNumber() {
		long hashCodeAsLong = hashCode();
		long integerMinAsLong = Integer.MIN_VALUE;
		return hashCodeAsLong + Math.abs(integerMinAsLong);
	}
}
