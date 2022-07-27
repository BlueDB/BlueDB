package org.bluedb.api.keys;

import java.util.UUID;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.index.BlueIndex;

/**
 * A key that can be mapped to a value in a {@link BlueCollection} or {@link BlueIndex}. Values inserted with
 * this key will be ordered by time. I-node usage will scale with the size of the timeframe that your data covers.
 * 
 * These function the same as {@link TimeKey}s except that they are treated by collections as active, meaning that
 * when you query for all values in a timeframe, these will be returned as if the end time was set to now since they
 * are considered still active or not ended. The time query API now also gives you the ability to include only
 * active records in a query or to exclude active records from a query. These only receive special treatment 
 * if the collection version is 2+.
 */
public final class ActiveTimeKey extends TimeKey {

	private static final long serialVersionUID = 1L;

	public ActiveTimeKey(long id, long time) {
		super(id, time);
	}

	public ActiveTimeKey(String id, long time) {
		super(id, time);
	}

	public ActiveTimeKey(UUID id, long time) {
		super(id, time);
	}

	public ActiveTimeKey(ValueKey key, long time) {
		super(key, time);
	}
	
	@Override
	public boolean overlapsRange(long min, long max) {
		return getGroupingNumber() <= max;
	}
	
	@Override
	public boolean isActiveTimeKey() {
		return true;
	}

	@Override
	public String toString() {
		return "ActiveTimeKey [getId()=" + getId() + ", getTime()=" + getTime() + "]";
	}
}
