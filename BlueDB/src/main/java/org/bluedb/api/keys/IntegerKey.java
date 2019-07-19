package org.bluedb.api.keys;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.index.BlueIndex;

/**
 * A key that can be mapped to a value in a {@link BlueCollection} or {@link BlueIndex}. Values inserted with
 * this key will be ordered and will fill up an entire segment before creating a new one. I-node usage will grow
 * as new segments are needed.
 */
public final class IntegerKey extends ValueKey {
	private static final long serialVersionUID = 1L;

	private final int id;

	public IntegerKey(int id) {
		this.id = id;
	}

	/**
	 * @return the id of this key
	 */
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
	public int postGroupingNumberCompareTo(BlueKey other) {
		if(other instanceof IntegerKey) {
			return Integer.compare(id, ((IntegerKey)other).id);
		} 
		return compareCanonicalClassNames(other);
	}

	@Override
	public Integer getIntegerIdIfPresent() {
		return id;
	}
}
