package org.bluedb.api.keys;

import java.util.UUID;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.index.BlueIndex;

/**
 * A key that can be mapped to a value in a {@link BlueCollection} or {@link BlueIndex}. Use this key when
 * your values have a start and end time and you need to be able to query all values that overlap a given
 * timeframe. BlueDB has to know the end time in order to fulfill that need, but the end time it isn't really 
 * part of the primary key of the value. You should be able to use a {@link TimeKey} when finding, updating,
 * or deleting these values. Values inserted with this type of key will be ordered by start time. I-node usage 
 * will scale with the size of the timeframe that your data covers. Efficiency goes down when your individual
 * records span a really large timeframe. In that case you may want to consider specifying a larger segment 
 * size for your collection.
 */
public final class TimeFrameKey extends TimeKey {

	private static final long serialVersionUID = 1L;

	private final long endTime;

	public TimeFrameKey(long id, long startTime, long endTime) {
		super(id, startTime);
		validateEndTime(endTime);
		this.endTime = endTime;
	}

	public TimeFrameKey(String id, long startTime, long endTime) {
		super(id, startTime);
		validateEndTime(endTime);
		this.endTime = endTime;
	}

	public TimeFrameKey(UUID id, long startTime, long endTime) {
		super(id, startTime);
		validateEndTime(endTime);
		this.endTime = endTime;
	}

	public TimeFrameKey(ValueKey key, long startTime, long endTime) {
		super(key, startTime);
		validateEndTime(endTime);
		this.endTime = endTime;
	}

	private void validateEndTime(long endTime) {
		if (endTime < this.getStartTime()) {
			throw new IllegalArgumentException("TimeFrameKey endTime must be >= startTime.");
		}
	}

	/**
	 * @return the start time of this key
	 */
	public long getStartTime() {
		return super.getTime();
	}

	/**
	 * @return the end time of this key
	 */
	public long getEndTime() {
		return endTime;
	}

	@Override
	public boolean overlapsRange(long min, long max) {
		return endTime >= min && getStartTime() <= max;
	}
	
	@Override
	public boolean isAfterRange(long min, long max) {
		return getStartTime() > max && endTime > max;
	}

	@Override
	public String toString() {
		return "TimeFrameKey [key=" + getId() + ", time=[" + getStartTime() + ", " + endTime + "] ]";
	}
}
