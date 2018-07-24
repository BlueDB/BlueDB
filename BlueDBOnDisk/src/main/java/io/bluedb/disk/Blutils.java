package io.bluedb.disk;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import io.bluedb.api.Condition;
import io.bluedb.api.exceptions.BlueDbException;

public class Blutils {
	public static <X extends Serializable> boolean meetsConditions(List<Condition<X>> conditions, X object) {
		for (Condition<X> condition: conditions) {
			if (!condition.test(object)) {
				return false;
			}
		}
		return true;
	}

	public static long roundDownToMultiple(long value, long multiple) {
		if (value >= 0) {
			return value - (value % multiple);
		} else if (Long.MIN_VALUE + multiple > value) {  // don't overflow
			return Long.MIN_VALUE;
		} else {
			return (value + 1) - ((value + 1) % multiple) - multiple;
		}
	}

	public static <X, Y extends Comparable<Y>> void sortByMappedValue(List<X> values, Function<? super X, ? extends Y> mapper) {
		Comparator<X> comparator = new Comparator<X>() {
			@Override
			public int compare(X x1, X x2) {
				Y y1 = mapper.apply(x1);
				Y y2 = mapper.apply(x2);
				return y1.compareTo(y2);
			}
		};
		Collections.sort(values, comparator);
	}

	@FunctionalInterface
	public interface CheckedFunction<T, R> {
	   R apply(T t) throws BlueDbException;
	}
	
	public static <X, Y> List<Y> map(List<? extends X> values, CheckedFunction<? super X, ? extends Y> mapper) throws BlueDbException {
		List<Y> results = new ArrayList<>();
		for (X originalValue: values) {
			Y newValue = mapper.apply(originalValue);
			results.add(newValue);
		}
		return results;
	}

	public static <X> List<X> filter(List<X> values, Predicate<X> condition) {
		List<X> results = new ArrayList<>();
		for (X value: values) {
			if (condition.test(value)) {
				results.add(value);
			}
		}
		return results;
	}

	public static <X> int lastIndex(List<X> values, Predicate<X> condition) {
		int result = -1;
		for (int i = 0; i < values.size(); i++) {
			X value = values.get(i);
			if (condition.test(value)) {
				result = i;
			}
		}
		return result;
	}

	public static boolean trySleep(long timeMillis) {
		try {
			Thread.sleep(timeMillis);
			return true;
		} catch (InterruptedException e) {
			return false;
		}
	}

	public static void recursiveDelete(File file) {
		if (file.isDirectory()) {
			for (File f: file.listFiles()) {
				recursiveDelete(f);
			}
			file.delete();
		} else {
			file.delete();
		}
	}
}
