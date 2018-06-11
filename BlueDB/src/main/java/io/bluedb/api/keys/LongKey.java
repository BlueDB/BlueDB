package io.bluedb.api.keys;

public class LongKey implements BlueKey {
	private static final long serialVersionUID = 1L;

	private long key;

	public LongKey(long key) {
		this.key = key;
	}

	public long getKey() {
		return key;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (key ^ (key >>> 32));
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
		LongKey other = (LongKey) obj;
		if (key != other.key) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "LongKey [key=" + key + "]";
	}
}
