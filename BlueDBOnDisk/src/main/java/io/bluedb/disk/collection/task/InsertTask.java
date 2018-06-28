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
			PendingChange<T> change = collection.getRecoveryManager().saveInsert(key, value);
			List<Segment<T>> segments = collection.getSegmentManager().getAllSegments(key);
			for (Segment<T> segment: segments) {
				change.applyChange(segment);
			}
			collection.getRecoveryManager().removeChange(change);
		} catch (Throwable t) {
			// TODO rollback or try again?
			t.printStackTrace();
			throw new RuntimeException(); // TODO
		} finally {
		}
	}

	@Override
	public String toString() {
		return "<InsertTask for key " + key + " and value " + value + ">";
	}
}
