package org.bluedb.api.keys;

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
		
		if(getClass() == other.getClass()) {
			//First by grouping number
			int groupingNumberComparison = Long.compare(getGroupingNumber(), other.getGroupingNumber());
			if (groupingNumberComparison != 0) {
				return groupingNumberComparison;
			}

			//Second by id
			Object otherId = ((HashGroupedKey<?>) other).getId();
			try {
				@SuppressWarnings("unchecked")
				T otherIdAsT = (T) otherId;
				return BlueKey.compareWithNullsLast(getId(), otherIdAsT);
			} catch(Throwable t) {
				//If ids are of different types then just compare the class names
				return BlueKey.compareCanonicalClassNames(getId(), otherId);
			}
		} else {
			// Grouping numbers and ids are not comparable between different classes so just compare the classname
			return compareCanonicalClassNames(other);
		}
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
