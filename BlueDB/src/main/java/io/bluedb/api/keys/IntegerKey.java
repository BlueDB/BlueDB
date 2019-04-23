package io.bluedb.api.keys;

public final class IntegerKey extends ValueKey {
	private static final long serialVersionUID = 1L;

	private final int id;

	public IntegerKey(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}
	
	@Override
	public long getGroupingNumber() {
		// make them all positive for better file paths
		long hashCodeAsLong = hashCode();
		long integerMinAsLong = Integer.MIN_VALUE;
		return hashCodeAsLong + Math.abs(integerMinAsLong);  
	}
	
	@Override
	public int hashCode() {
		return id;
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
		IntegerKey other = (IntegerKey) obj;
		if (id != other.id) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "IntegerKey [key=" + id + "]";
	}

	@Override
	public int compareTo(BlueKey other) {
		if(other == null) {
			return -1;
		} else if(other instanceof IntegerKey) {
			return Integer.compare(id, ((IntegerKey)other).id);
		} else {
			// grouping number is not comparable between most subclasses
			return compareClasses(other);
		}		
	}

	@Override
	public Integer getIntegerIdIfPresent() {
		return id;
	}
}
