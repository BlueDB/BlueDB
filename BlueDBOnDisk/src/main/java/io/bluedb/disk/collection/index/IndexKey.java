package io.bluedb.disk.collection.index;

import io.bluedb.api.keys.BlueKey;
import static io.bluedb.disk.Blutils.nullSafeEquals;

public class IndexKey<K extends BlueKey> implements BlueKey {

	private static final long serialVersionUID = 1L;

	private final K indexKey;
	private final BlueKey targetKey;

	public IndexKey(K indexKey, BlueKey targetKey) {
		this.indexKey = indexKey;
		this.targetKey = targetKey;
	}

	@Override
	public int compareTo(BlueKey other) {
		if(other == null) {
			return -1;
		}
		
		if(other instanceof IndexKey) {
			IndexKey<?> otherIndexKey = (IndexKey<?>)other;
			int indexComparison = indexKey.compareTo(otherIndexKey.indexKey);
			if (indexComparison == 0) {
				return targetKey.compareTo(otherIndexKey.targetKey);
			} else {
				return indexComparison;
			}
		}
		
		return getClass().getSimpleName().compareTo(other.getClass().getSimpleName());
	}

	@Override
	public long getGroupingNumber() {
		return indexKey.getGroupingNumber();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((indexKey == null) ? 0 : indexKey.hashCode());
		result = prime * result + ((targetKey == null) ? 0 : targetKey.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof IndexKey)) {
			return false;
		}
		IndexKey<?> other = (IndexKey<?>) obj;
		nullSafeEquals(null, null);
		return nullSafeEquals(this.indexKey, other.indexKey) && nullSafeEquals(this.targetKey, other.targetKey);
	}

	@Override
	public String toString() {
		return "IndexKey [" + indexKey + " -> " + targetKey  + "]";
	}

	@Override
	public boolean isInRange(long min, long max) {
		return indexKey.isInRange(min, max);
	}

	public BlueKey getTargetKey() {
		return targetKey;
	}
}
