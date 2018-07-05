package io.bluedb.disk;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import io.bluedb.api.Condition;

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
		return value - (value % multiple);
	}

	public static <X extends Serializable> List<X> filter(List<X> values, Predicate<X> condition) {
		List<X> results = new ArrayList<>();
		for (X value: values) {
			if (condition.test(value)) {
				results.add(value);
			}
		}
		return results;
	}
}
