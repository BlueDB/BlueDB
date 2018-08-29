package io.bluedb.disk.collection.index;

import io.bluedb.api.keys.BlueKey;
import static io.bluedb.disk.Blutils.nullSafeEquals;

public class IndexCompositeKey<K extends BlueKey> implements BlueKey {

	private static final long serialVersionUID = 1L;

	private final K indexKey;
	private final BlueKey valueKey;

	public IndexCompositeKey(K indexKey, BlueKey valueKey) {
		this.indexKey = indexKey;
		this.valueKey = valueKey;
	}

	@Override
	public int compareTo(BlueKey other) {
		if(other == null) {
			return -1;
		}
		
		if(other instanceof IndexCompositeKey) {
			IndexCompositeKey<?> otherIndexKey = (IndexCompositeKey<?>)other;
			int indexComparison = indexKey.compareTo(otherIndexKey.indexKey);
			if (indexComparison == 0) {
				return valueKey.compareTo(otherIndexKey.valueKey);
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
		result = prime * result + ((valueKey == null) ? 0 : valueKey.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof IndexCompositeKey)) {
			return false;
		}
		IndexCompositeKey<?> other = (IndexCompositeKey<?>) obj;
		nullSafeEquals(null, null);
		return nullSafeEquals(this.indexKey, other.indexKey) && nullSafeEquals(this.valueKey, other.valueKey);
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName() + " [" + indexKey + " -> " + valueKey  + "]";
	}

	@Override
	public boolean isInRange(long min, long max) {
		return indexKey.isInRange(min, max);
	}

	public BlueKey getValueKey() {
		return valueKey;
	}
}
