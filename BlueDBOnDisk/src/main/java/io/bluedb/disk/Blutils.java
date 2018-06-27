package io.bluedb.disk;

import java.io.Serializable;
import java.util.List;

import io.bluedb.api.Condition;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.api.keys.TimeKey;

public class Blutils {
		public static <X extends Serializable> boolean meetsConditions(List<Condition<X>> conditions, X object) {
		for (Condition<X> condition: conditions) {
			if (!condition.test(object)) {
				return false;
			}
		}
		return true;
	}

	public static boolean meetsTimeConstraint(BlueKey key, long minTime, long maxTime) {
		if (key instanceof TimeFrameKey) {
			TimeFrameKey timeKey = (TimeFrameKey) key;
			return timeKey.getEndTime() >= minTime && timeKey.getStartTime() <= maxTime;
		}
		if (key instanceof TimeKey) {
			TimeKey timeKey = (TimeKey) key;
			return timeKey.getTime() >= minTime && timeKey.getTime() <= maxTime;
		}
		return true;
	}
}
