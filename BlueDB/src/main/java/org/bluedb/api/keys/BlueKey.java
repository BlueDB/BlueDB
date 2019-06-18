package org.bluedb.api.keys;

import java.io.Serializable;
import java.util.Comparator;

/**
 * This class and subclasses are used as keys that may map to a value in a BlueCollection or BlueIndex.
 */
public interface BlueKey extends Serializable, Comparable<BlueKey> {

	static final Comparator<Object> nullSafeClassComparator = Comparator.nullsLast(BlueKey::unsafeCompareCanonicalClassNames);

	@Override
	public abstract int hashCode();

	@Override
	public abstract boolean equals(Object object);

	@Override
	public abstract int compareTo(BlueKey other);
	
	/**
	 * Returns the grouping number.  For a TimeKey, this is the Unix timestamp.
	 * @return grouping number
	 */
	public long getGroupingNumber();
	
	/**
	 * Convenience method to get the underlying Long value if this is a LongKey
	 * @return the underlying Long value if this is a LongKey
	 */
	default public Long getLongIdIfPresent() {
		return null;
	}

	/**
	 * Convenience method to get the underlying Integer value if this is a IntegerKey
	 * @return the underlying Integer value if this is a IntegerKey
	 */
	default public Integer getIntegerIdIfPresent() {
		return null;
	}

	/**
	 * Returns true if this key's grouping number is in the range [min, max] inclusive, else false
	 * @param min the minimum grouping number
	 * @param max the maximum grouping number
	 * @return true if this key's grouping number is in the range [min, max] inclusive, else false
	 */
	default boolean isInRange(long min, long max) {
		return getGroupingNumber() >= min && getGroupingNumber() <= max;
	}

	/**
	 * Compares the canonical class name so that two keys of different subtypes can be ordered consistently.
	 * @param other the key that this is being compared against
	 * @return 0 if this has the same class as other, or else a negative or positive int to indicate relative order
	 */
	default int compareCanonicalClassNames(BlueKey other) {
		return nullSafeClassComparator.compare(this, other);
	}

	/**
	 * Compares the canonical class name so that two keys of different subtypes can be ordered consistently.
	 * @param first key to compare against second
	 * @param second key to compare against first
	 * @return 0 if the classes are the same, a negative number if first should come first and a positive number if second should
	 */
	public static int compareCanonicalClassNames(Object first, Object second) {
		return nullSafeClassComparator.compare(first,  second);
	}

	/**
	 * Compares the canonical class name so that two keys of different subtypes can be ordered consistently.
	 * @param first key to compare against second
	 * @param second key to compare against first
	 * @return 0 if the classes are the same, a negative number if first should come first and a positive number if second should
	 */
	public static int unsafeCompareCanonicalClassNames(Object first, Object second) {
		String firstClassName = first.getClass().getCanonicalName();
		String secondClassName = second.getClass().getCanonicalName();
		return firstClassName.compareTo(secondClassName);
	}
	
	/**
	 * Compares the the values in a null-safe way
	 * @param item1 value to compare against item2
	 * @param item2 value to compare against item1
	 * @return 0 if the classes are the same, a negative number if first should come first and a positive number if second should
	 */
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
