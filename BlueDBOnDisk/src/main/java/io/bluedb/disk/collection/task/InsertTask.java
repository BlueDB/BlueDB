package io.bluedb.disk.collection.task;

import java.io.Serializable;
import java.util.List;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.exceptions.DuplicateKeyException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.collection.BlueCollectionImpl;
import io.bluedb.disk.recovery.PendingChange;
import io.bluedb.disk.segment.Segment;

public class InsertTask<T extends Serializable> implements Runnable {

	private final BlueCollectionImpl<T> collection;
	private final BlueKey key;
	private final T value;
	
	public InsertTask(BlueCollectionImpl<T> collection, BlueKey key, T value) {
		this.collection = collection;
		this.key = key;
		this.value = value;
	}

	@Override
	public void run() {
		try {
			if (collection.contains(key)) {
				throw new DuplicateKeyException("key already exists: " + key);
			}
			PendingChange<T> change = PendingChange.createInsert(key, value);
			applyUpdateWithRecovery(key, change);
		} catch (Throwable t) {
			// TODO rollback or try again?
			throw new RuntimeException(); // TODO
		} finally {
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
