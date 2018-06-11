package io.bluedb.api.keys;

public class TimeKey implements BlueKey {
	private static final long serialVersionUID = 1L;

	private BlueKey id;
	private long time;

	public TimeKey(int id, long time) {
		this(new IntegerKey(id), time);
	}

	public TimeKey(long id, long time) {
		this(new LongKey(id), time);
	}

	public TimeKey(String id, long time) {
		this(new StringKey(id), time);
	}

	public TimeKey(BlueKey id, long time) {
		this.id = id;
		this.time = time;
	}

	public BlueKey getId() {
		return id;
	}

	public long getTime() {
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
}
