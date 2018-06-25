package io.bluedb.disk.collection.task;

import java.io.Serializable;
import java.util.List;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.collection.BlueCollectionImpl;
import io.bluedb.disk.recovery.PendingChange;
import io.bluedb.disk.segment.Segment;

public class DeleteTask<T extends Serializable> implements Runnable {
	private final BlueCollectionImpl<T> collection;
	private final BlueKey key;
	
	public DeleteTask(BlueCollectionImpl<T> collection, BlueKey key) {
		this.collection = collection;
		this.key = key;
	}

	@Override
	public void run() {
		try {
			PendingChange<T> change = PendingChange.createDelete(key);
			applyUpdateWithRecovery(key, change);
		} catch (Throwable t) {
			// TODO rollback or try again?
		} finally {
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
}
