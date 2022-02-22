package org.bluedb.disk.collection.index;

import org.bluedb.api.keys.BlueKey;
import java.util.Objects;

public class IndexCompositeKey<K extends BlueKey> implements BlueKey {

	private static final long serialVersionUID = 1L;

	private final K indexKey;
	private final BlueKey valueKey;

	public IndexCompositeKey(K indexKey, BlueKey valueKey) {
		this.indexKey = indexKey;
		this.valueKey = valueKey;
	}

	@Override
	public int postGroupingNumberCompareTo(BlueKey other) {
		if(other instanceof IndexCompositeKey) {
			IndexCompositeKey<?> otherIndexKey = (IndexCompositeKey<?>)other;
			int indexComparison = indexKey.compareTo(otherIndexKey.indexKey);
			if (indexComparison == 0) {
				return valueKey.compareTo(otherIndexKey.valueKey);
			} else {
				return indexComparison;
			}
		}
		
		return compareCanonicalClassNames(other);
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
		return Objects.equals(this.indexKey, other.indexKey) && Objects.equals(this.valueKey, other.valueKey);
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName() + " [" + indexKey + " -> " + valueKey  + "]";
	}
	
	@Override
	public boolean isBeforeRange(long min, long max) {
		return indexKey.isBeforeRange(min, max);
	}

	@Override
	public boolean isInRange(long min, long max) {
		return indexKey.isInRange(min, max);
	}
	
	@Override
	public boolean isAfterRange(long min, long max) {
		return indexKey.isAfterRange(min, max);
	}

	public K getIndexKey() {
		return indexKey;
	}

	public BlueKey getValueKey() {
		return valueKey;
	}
}
