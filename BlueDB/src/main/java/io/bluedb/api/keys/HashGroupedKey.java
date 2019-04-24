package io.bluedb.api.keys;

@SuppressWarnings("serial")
public abstract class HashGroupedKey<T extends Comparable<T>> extends ValueKey {

	public abstract T getId();

	@Override
	public final int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((getId() == null) ? 0 : getId().hashCode());
		return result;
	}


	@Override
	public final String toString() {
		return getClass().getSimpleName() + " [key=" + getId() + "]";
	}

	@Override
	public final int compareTo(BlueKey other) {
		if(other == null) {
			return -1;
		}
		// we compare classes before grouping number because grouping number is not comparable between classes
		int classComparison = compareClasses(other);
		if (classComparison != 0) {
			return classComparison;
		}
		HashGroupedKey<?> otherAsHashGroupedKey = (HashGroupedKey<?>) other;
		Object otherId = otherAsHashGroupedKey.getId();
		int idClassCompare = BlueKey.compareCanonicalClassNames(getId(), otherId);
		if (idClassCompare != 0) {
			return idClassCompare;
		}
		int groupingNumberComparison = Long.compare(getGroupingNumber(), other.getGroupingNumber());
		if (groupingNumberComparison != 0) {
			return groupingNumberComparison;
		}
		@SuppressWarnings("unchecked")
		T otherIdAsT = (T) otherId;
		return getId().compareTo(otherIdAsT);
	}

	@Override
	public final long getGroupingNumber() {
		long hashCodeAsLong = hashCode();
		long integerMinAsLong = Integer.MIN_VALUE;
		return hashCodeAsLong + Math.abs(integerMinAsLong);
	}


	@Override
	public final boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		HashGroupedKey<?> other = (HashGroupedKey<?>) obj;
		if (getId() == null) {
			return other.getId() == null;
		}
		return getId().equals(other.getId());
	}
}
