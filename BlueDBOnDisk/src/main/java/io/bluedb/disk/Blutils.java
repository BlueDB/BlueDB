package io.bluedb.disk;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import io.bluedb.api.Condition;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeFrameKey;

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

	public static <X extends Serializable> List<X> filter(List<X> values, Predicate<X> condition) {
		List<X> results = new ArrayList<>();
		for (X value: values) {
			if (condition.test(value)) {
				results.add(value);
			}
		}
		return results;
	}

	public static boolean isInRange(BlueKey key, long min, long max) {
		if (key instanceof TimeFrameKey) {
			TimeFrameKey timeFrameKey = (TimeFrameKey) key;
			return timeFrameKey.getEndTime() >= min && timeFrameKey.getStartTime() <= max;
		} else {
			return key.getGroupingNumber() >= min && key.getGroupingNumber() <= max;
		}
	}
}
