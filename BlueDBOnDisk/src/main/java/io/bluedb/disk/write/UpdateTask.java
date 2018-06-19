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

public class UpdateTask<T extends Serializable> implements Runnable {
	private final RecoveryManager recoveryManager;
	private final BlueCollectionImpl<T> collection;
	private final BlueKey key;
	private final Updater<T> updater;
	
	public UpdateTask(RecoveryManager recoveryManager, BlueCollectionImpl<T> collection, BlueKey key, Updater<T> updater) {
		this.collection = collection;
		this.recoveryManager = recoveryManager;
		this.key = key;
		this.updater = updater;
	}

	@Override
	public void run() {
		try {
			T value = collection.get(key);
			PendingChange change = PendingChange.createUpdate(key, value, updater);
			applyUpdateWithRecovery(key, change);
		} catch (BlueDbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
