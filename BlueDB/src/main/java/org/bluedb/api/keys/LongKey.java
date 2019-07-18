package org.bluedb.api.keys;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.index.BlueIndex;

/**
 * A key that can be mapped to a value in a {@link BlueCollection} or {@link BlueIndex}. Values inserted with
 * this key will be ordered and will fill up an entire segment before creating a new one. I-node usage will grow
 * as new segments are needed.
 */
public final class LongKey extends ValueKey {
	private static final long serialVersionUID = 1L;

	private final long id;

	public LongKey(long id) {
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
		return (id / 2) - (Long.MIN_VALUE / 2);  // make them all positive for better file paths
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
		// grouping number is not comparable between most subclasses
		return compareCanonicalClassNames(other);
	}

	@Override
	public Long getLongIdIfPresent() {
		return id;
	}
}
