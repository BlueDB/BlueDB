package io.bluedb.disk.write;

import java.io.Serializable;
import java.util.List;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.exceptions.DuplicateKeyException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.collection.BlueCollectionImpl;
import io.bluedb.disk.recovery.PendingChange;
import io.bluedb.disk.recovery.RecoveryManager;
import io.bluedb.disk.segment.Segment;

public class InsertTask<T extends Serializable> implements Runnable {

	private final RecoveryManager recoveryManager;
	private final BlueCollectionImpl<T> collection;
	private final BlueKey key;
	private final T value;
	
	public InsertTask(RecoveryManager recoveryManager, BlueCollectionImpl<T> collection, BlueKey key, T value) {
		this.collection = collection;
		this.recoveryManager = recoveryManager;
		this.key = key;
		this.value = value;
	}

	@Override
	public void run() {
		try {
			if (collection.contains(key)) {
				throw new DuplicateKeyException("key already exists: " + key);
			}
			PendingChange change = PendingChange.createInsert(key, value);
			applyUpdateWithRecovery(key, change);
		} catch (Throwable t) {
			// TODO rollback or try again?
			throw new RuntimeException(); // TODO
		} finally {
		}
	}

	private void applyUpdateWithRecovery(BlueKey key, PendingChange change) throws BlueDbException {
		recoveryManager.saveChange(change);
		List<Segment> segments = collection.getSegments(key);
		for (Segment segment: segments) {
			change.applyChange(segment);
		}
		recoveryManager.removeChange(change);
	}
}
