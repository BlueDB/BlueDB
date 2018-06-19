package io.bluedb.disk.write;

import java.io.Serializable;
import java.util.List;
import io.bluedb.api.Updater;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.collection.BlueCollectionImpl;
import io.bluedb.disk.recovery.PendingChange;
import io.bluedb.disk.recovery.RecoveryManager;
import io.bluedb.disk.segment.Segment;

public class DeleteTask<T extends Serializable> implements Runnable {
	private final RecoveryManager recoveryManager;
	private final BlueCollectionImpl<T> collection;
	private final BlueKey key;
	
	public DeleteTask(RecoveryManager recoveryManager, BlueCollectionImpl<T> collection, BlueKey key) {
		this.collection = collection;
		this.recoveryManager = recoveryManager;
		this.key = key;
	}

	@Override
	public void run() {
		try {
			PendingChange change = PendingChange.createDelete(key);
			applyUpdateWithRecovery(key, change);
		} catch (Throwable t) {
			// TODO rollback or try again?
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
