package io.bluedb.disk.collection.task;

import java.io.Serializable;
import java.util.List;
import io.bluedb.api.Updater;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.collection.BlueCollectionImpl;
import io.bluedb.disk.recovery.PendingChange;
import io.bluedb.disk.recovery.RecoveryManager;
import io.bluedb.disk.segment.BlueEntity;
import io.bluedb.disk.segment.Segment;

public class UpdateMultipleTask<T extends Serializable> implements Runnable {
	private final RecoveryManager recoveryManager;
	private final BlueCollectionImpl<T> collection;
	private final List<BlueEntity<T>> entities;
	private final Updater<T> updater;
	
	public UpdateMultipleTask(RecoveryManager recoveryManager, BlueCollectionImpl<T> collection, List<BlueEntity<T>> entities, Updater<T> updater) {
		this.collection = collection;
		this.recoveryManager = recoveryManager;
		this.entities = entities;
		this.updater = updater;
	}

	@Override
	public void run() {
		try {
			for (BlueEntity<T> entity: entities) {
				BlueKey key = entity.getKey();
				T value = (T) entity.getObject();
				PendingChange change = PendingChange.createUpdate(key, value, updater);
				applyUpdateWithRecovery(key, change);
				// TODO probably make it fail before doing any updates if any update fails?
			}
		} catch (BlueDbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void applyUpdateWithRecovery(BlueKey key, PendingChange change) throws BlueDbException {
		recoveryManager.saveChange(change);
		List<Segment<T>> segments = collection.getSegments(key);
		for (Segment<T> segment: segments) {
			change.applyChange(segment);
		}
		recoveryManager.removeChange(change);
	}
}
