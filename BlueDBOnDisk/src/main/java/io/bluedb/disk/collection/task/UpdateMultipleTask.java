package io.bluedb.disk.collection.task;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import io.bluedb.api.Condition;
import io.bluedb.api.Updater;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.collection.BlueCollectionImpl;
import io.bluedb.disk.recovery.PendingChange;
import io.bluedb.disk.recovery.RecoveryManager;
import io.bluedb.disk.segment.BlueEntity;
import io.bluedb.disk.serialization.BlueSerializer;

public class UpdateMultipleTask<T extends Serializable> extends QueryTask {
	private final BlueCollectionImpl<T> collection;
	private final Updater<T> updater;
	private final long min;
	private final long max;
	private final  List<Condition<T>> conditions;


	public UpdateMultipleTask(BlueCollectionImpl<T> collection, long min, long max, List<Condition<T>> conditions, Updater<T> updater) {
		this.collection = collection;
		this.min = min;
		this.max = max;
		this.conditions = conditions;
		this.updater = updater;
	}

	@Override
	public void execute() throws BlueDbException {
		List<BlueEntity<T>> entities = collection.findMatches(min, max, conditions);
		List<PendingChange<T>> updates;
		try {
			updates = createUpdates(entities, updater);
		} catch(Throwable t) {
			t.printStackTrace();
			throw new BlueDbException("Error updating values", t);
		}

		RecoveryManager<T> recoveryManager = collection.getRecoveryManager();
		for (PendingChange<T> update: updates) {
			recoveryManager.saveChange(update);
			collection.applyChange(update);
			recoveryManager.removeChange(update);
		}
	}

	private List<PendingChange<T>> createUpdates(List<BlueEntity<T>> entities, Updater<T> updater) {
		BlueSerializer serializer = collection.getSerializer();

		List<PendingChange<T>> updates = new ArrayList<>();
		for (BlueEntity<T> entity: entities) {
			PendingChange<T> update = PendingChange.createUpdate(entity, updater, serializer);
			updates.add(update);
		}
		return updates;
	}

	@Override
	public String toString() {
		return "<UpdateMultipleTask [" + min + ", " + max + "] with " + conditions.size() + " conditions>";
	}
}
