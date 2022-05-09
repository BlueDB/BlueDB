package org.bluedb.api.keys;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.index.BlueIndex;

/**
 * A key that can be mapped to a value in a {@link BlueCollection} or {@link BlueIndex}. Values inserted with
 * this key will be ordered and will fill up an entire segment before creating a new one. I-node usage will grow
 * as new segments are needed.
 * 
 * This is almost identical to {@link LongKey} except that it will use time segment sizes and path management. This
 * key will automatically be used in the default time index on collections containing records that cover a timeframe.
 * An index on a time in milliseconds field should use much larger segment sizes than an index on a long field that
 * represents an id. Otherwise the i-node usage gets really out of hand.
 */
public final class LongTimeKey extends ValueKey {
	private static final long serialVersionUID = 1L;

	private final long id;

	public LongTimeKey(long id) {
		this.id = id;
	}

	/**
	 * @return the id of this key
	 */
	public long getId() {
		return id;
	}
	
	@Override
	public long getGroupingNumber() {
		return id;
	}

	@Override
	public int hashCode() {
		return (int) (id ^ (id >>> 32));
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
		LongTimeKey other = (LongTimeKey) obj;
		if (id != other.id) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "LongTimeKey [id=" + id + "]";
	}

	@Override
	public int postGroupingNumberCompareTo(BlueKey other) {
		if(other instanceof LongTimeKey) {
			return Long.compare(id, ((LongTimeKey)other).id);
		} 
		return compareCanonicalClassNames(other);
	}

	@Override
	public Long getLongIdIfPresent() {
		return id;
	}
}
