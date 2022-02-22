package org.bluedb.api.keys;

import java.util.UUID;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.index.BlueIndex;

/**
 * A key that can be mapped to a value in a {@link BlueCollection} or {@link BlueIndex}. Values inserted with
 * this key will be ordered by start time. I-node usage will scale with the size of the timeframe that your data covers.
 */
public final class TimeFrameKey extends TimeKey {
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (int) (endTime ^ (endTime >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (this == obj)
			return true;
		if (getClass() != obj.getClass())
			return false;
		if (!super.equals(obj))
			return false;
		TimeFrameKey other = (TimeFrameKey) obj;
		if (endTime != other.endTime)
			return false;
		return true;
	}
	
	@Override
	public boolean isBeforeRange(long min, long max) {
		return getStartTime() < min && endTime < min;
	}

	@Override
	public boolean isInRange(long min, long max) {
		return endTime >= min && getStartTime() <= max;
	}
	
	@Override
	public boolean isAfterRange(long min, long max) {
		return getStartTime() > max && endTime > max;
	}

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
	public String toString() {
		return "TimeFrameKey [key=" + getId() + ", time=[" + getStartTime() + ", " + endTime + "] ]";
	}

	private void validateEndTime(long endTime) {
		if (endTime < this.getStartTime()) {
			throw new IllegalArgumentException("TimeFrameKey endTime must be >= startTime.");
		}
	}
}
