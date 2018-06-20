package io.bluedb.disk.collection.task;

import java.io.Serializable;
import java.util.List;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.collection.BlueCollectionImpl;
import io.bluedb.disk.recovery.PendingChange;
import io.bluedb.disk.segment.Segment;

public class DeleteMultipleTask<T extends Serializable> implements Runnable {
	private final BlueCollectionImpl<T> collection;
	private final List<BlueKey> keys;
	
	public DeleteMultipleTask(BlueCollectionImpl<T> collection, List<BlueKey> keys) {
		this.collection = collection;
		this.keys = keys;
	}

	@Override
	public void run() {
		try {
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
		List<Segment<T>> segments = collection.getSegments(key);
		for (Segment<T> segment: segments) {
			change.applyChange(segment);
		}
		collection.getRecoveryManager().removeChange(change);
	}
}
