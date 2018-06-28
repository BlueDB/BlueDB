package io.bluedb.disk.collection.task;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import io.bluedb.api.Condition;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.collection.BlueCollectionImpl;
import io.bluedb.disk.recovery.PendingChange;
import io.bluedb.disk.segment.Segment;

public class DeleteMultipleTask<T extends Serializable> implements Runnable {
	private final BlueCollectionImpl<T> collection;
	private final long minGroupingValue;
	private final long maxGroupingValue;
	private final List<Condition<T>> conditions;
	
	public DeleteMultipleTask(BlueCollectionImpl<T> collection, long min, long max, List<Condition<T>> conditions) {
		this.collection = collection;
		this.minGroupingValue = min;
		this.maxGroupingValue = max;
		this.conditions = conditions;
	}

	@Override
	public void run() {
		try {
			List<BlueKey> keys = collection.findMatches(minGroupingValue, maxGroupingValue, conditions).stream()
					.map((e) -> e.getKey())
					.collect(Collectors.toList());
			for (BlueKey key: keys) {
				PendingChange<T> change = PendingChange.createDelete(key);
				applyUpdateWithRecovery(key, change);
			}
		} catch (BlueDbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void applyUpdateWithRecovery(BlueKey key, PendingChange<T> change) throws BlueDbException {
		collection.getRecoveryManager().saveChange(change);
		List<Segment<T>> segments = collection.getSegmentManager().getAllSegments(key);
		for (Segment<T> segment: segments) {
			change.applyChange(segment);
		}
		collection.getRecoveryManager().removeChange(change);
	}

	@Override
	public String toString() {
		return "<DeleteMultipleTask [" + minGroupingValue + ", " + maxGroupingValue + "] with " + conditions.size() + " conditions>";
		return "<DeleteMultipleTask [" + minGroupingValue + ", " + maxGroupingValue + "] with " + conditions.size() + " conditions>";
	}
}
