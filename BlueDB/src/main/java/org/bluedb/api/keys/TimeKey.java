package org.bluedb.api.keys;

import java.util.UUID;

public class TimeKey implements BlueKey {
	private static final long serialVersionUID = 1L;

	private final ValueKey id;
	private final long time;

	public TimeKey(long id, long time) {
		this(new LongKey(id), time);
	}

	public TimeKey(String id, long time) {
		this(new StringKey(id), time);
	}

	public TimeKey(UUID id, long time) {
		this(new UUIDKey(id), time);
	}

	public TimeKey(ValueKey id, long time) {
		this.id = id;
		this.time = time;
	}

	public ValueKey getId() {
		return id;
	}

	public long getTime() {
		return time;
	}
	
	@Override
	public final long getGroupingNumber() {
		return time;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + (int) (time ^ (time >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		TimeKey other = (TimeKey) obj;
		if (id == null) {
			if (other.id != null) {
				return false;
			}
		} else if (!id.equals(other.id)) {
			return false;
		}
		if (time != other.time) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "TimeKey [key=" + id + ", time=" + time + "]";
	}

	@Override
	public final int compareTo(BlueKey other) {
		if(other == null) {
			return -1;
		}
		
		if (other instanceof TimeKey) {
			long otherTime = ((TimeKey)other).getTime();
			if (getTime() == otherTime) {
				return id.compareTo(((TimeKey) other).getId());
			} else if (getTime() > otherTime) {
				return 1;
			} else {
				return -1;
			}
		}
		// grouping number is not comparable between most subclasses
		return compareCanonicalClassNames(other);
	}


	@Override
	public Integer getIntegerIdIfPresent() {
		return id.getIntegerIdIfPresent();
	}

	@Override
	public Long getLongIdIfPresent() {
		return id.getLongIdIfPresent();
	}
}
