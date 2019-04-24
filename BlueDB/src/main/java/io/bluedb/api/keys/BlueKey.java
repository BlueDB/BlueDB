package io.bluedb.api.keys;

import java.io.Serializable;
import java.util.Comparator;

public interface BlueKey extends Serializable, Comparable<BlueKey> {

	static final Comparator<Object> nullSafeClassComparator = Comparator.nullsLast(BlueKey::unsafeCompareCanonicalClassNames);

	@Override
	public abstract int hashCode();

	@Override
	public abstract boolean equals(Object object);

	@Override
	public abstract int compareTo(BlueKey other);
	
	public long getGroupingNumber();
	
	default public Long getLongIdIfPresent() {
		return null;
	}

	default public Integer getIntegerIdIfPresent() {
		return null;
	}

	default boolean isInRange(long min, long max) {
		return getGroupingNumber() >= min && getGroupingNumber() <= max;
	}

	default int compareCanonicalClassNames(BlueKey other) {
		return nullSafeClassComparator.compare(this, other);
	}

	public static int compareCanonicalClassNames(Object first, Object second) {
		return nullSafeClassComparator.compare(first,  second);
	}

	public static int unsafeCompareCanonicalClassNames(Object first, Object second) {
		String firstClassName = first.getClass().getCanonicalName();
		String secondClassName = second.getClass().getCanonicalName();
		return firstClassName.compareTo(secondClassName);
	}
	
	public static <T extends Comparable<T>> int compareWithNullsLast(T item1, T item2) {
		if(item1 != null && item2 != null) {
			return item1.compareTo(item2);
		}
		else if(item1 == null && item2 != null) {
			return 1;
		}
		else if(item1 != null && item2 == null) {
			return -1;
		}
		return 0;
	}
}
