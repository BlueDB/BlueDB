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
	private final BlueCollectionImpl<T> collection;
	private final List<BlueEntity<T>> entities;
	private final Updater<T> updater;
	
	public UpdateMultipleTask(BlueCollectionImpl<T> collection, List<BlueEntity<T>> entities, Updater<T> updater) {
		this.collection = collection;
		this.entities = entities;
		this.updater = updater;
	}

	@Override
	public void run() {
		try {
			for (BlueEntity<T> entity: entities) {
				BlueKey key = entity.getKey();
				T value = (T) entity.getObject();
				RecoveryManager<T> recoveryManager = collection.getRecoveryManager();
				PendingChange<T> change = recoveryManager.saveUpdate(key, value, updater);
				List<Segment<T>> segments = collection.getSegmentManager().getAllSegments(key);
				for (Segment<T> segment: segments) {
					change.applyChange(segment);
				}
				collection.getRecoveryManager().removeChange(change);
				// TODO probably make it fail before doing any updates if any update fails?
			}
		} catch (BlueDbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public String toString() {
		return "<UpdateMultipleTask for " + entities.size() + " keys>";
	}
}
