package org.bluedb.api.keys;

@SuppressWarnings("serial")
public abstract class ValueKey implements BlueKey {
	@Override
	public final boolean isInRange(long min, long max) {
		return getGroupingNumber() >= min && getGroupingNumber() <= max;
	}
}
