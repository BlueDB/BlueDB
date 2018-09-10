package io.bluedb.api.keys;

import java.util.UUID;

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
	public boolean isInRange(long min, long max) {
		return endTime >= min && getStartTime() <= max;
	}

	private static final long serialVersionUID = 1L;

	private long endTime;

	public TimeFrameKey(long id, long startTime, long endTime) {
		super(id, startTime);
		this.endTime = endTime;
	}

	public TimeFrameKey(String id, long startTime, long endTime) {
		super(id, startTime);
		this.endTime = endTime;
	}

	public TimeFrameKey(UUID id, long startTime, long endTime) {
		super(id, startTime);
		this.endTime = endTime;
	}

	public TimeFrameKey(ValueKey key, long startTime, long endTime) {
		super(key, startTime);
		this.endTime = endTime;
	}

	public long getStartTime() {
		return super.getTime();
	}

	public long getEndTime() {
		return endTime;
	}

	@Override
	public String toString() {
		return "TimeFrameKey [key=" + getId() + ", time=[" + getStartTime() + ", " + endTime + "] ]";
	}
}
