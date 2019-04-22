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
		int groupingNumberComparison = Long.compare(getGroupingNumber(), other.getGroupingNumber());
		if (groupingNumberComparison != 0) {
			return groupingNumberComparison;
		}
		int classComparison = getClass().getSimpleName().compareTo(other.getClass().getSimpleName());
		if (classComparison != 0) {
			return classComparison;
		}
		@SuppressWarnings("unchecked")
		HashGroupedKey<T> otherClassed = (HashGroupedKey<T>) other;
		return getId().compareTo(otherClassed.getId());
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
		@SuppressWarnings("unchecked")
		HashGroupedKey<T> other = (HashGroupedKey<T>) obj;
		if (getId() == null) {
			return other.getId() == null;
		}
		return getId().equals(other.getId());
	}
}
