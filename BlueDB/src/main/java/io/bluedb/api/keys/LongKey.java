package io.bluedb.api.keys;

public class LongKey implements ValueKey {
	private static final long serialVersionUID = 1L;

	private long id;

	public LongKey(long id) {
		this.id = id;
	}

	public long getId() {
		return id;
	}
	
	@Override
	public long getGroupingNumber() {
		return hashCode();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
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
		if (id != other.id) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "LongKey [key=" + id + "]";
	}

	@Override
	public int compareTo(BlueKey other) {
		if(other == null) {
			return -1;
		}
		
		if(other instanceof LongKey) {
			long otherId = ((LongKey)other).id;
			if(id < otherId) {
				return -1;
			}
			if(id > otherId) {
				return 1;
			}
		}
		
		return getClass().getSimpleName().compareTo(other.getClass().getSimpleName());
	}

	@Override
	public Long getLongIdIfPresent() {
		return id;
	}
}
