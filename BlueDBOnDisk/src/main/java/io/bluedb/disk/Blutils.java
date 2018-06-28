package io.bluedb.disk;

import java.io.Serializable;
import java.util.List;

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
}
