package org.bluedb.disk.collection.task;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bluedb.api.Updater;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.collection.ReadOnlyBlueCollectionOnDisk;
import org.bluedb.disk.query.ReadOnlyBlueQueryOnDisk;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.recovery.PendingBatchChange;
import org.bluedb.disk.recovery.RecoveryManager;
import org.bluedb.disk.serialization.BlueEntity;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.validation.SerializationException;

public class UpdateMultipleTask<T extends Serializable> extends QueryTask {
	private final ReadOnlyBlueCollectionOnDisk<T> collection;
	private final ReadOnlyBlueQueryOnDisk<T> query;
	private final Updater<T> updater;


	public UpdateMultipleTask(ReadOnlyBlueCollectionOnDisk<T> collection, ReadOnlyBlueQueryOnDisk<T> query, Updater<T> updater) {
		this.collection = collection;
		this.query = query;
		this.updater = updater;
	}

	@Override
	public void execute() throws BlueDbException {
		List<BlueEntity<T>> entities = query.getEntities();
		List<IndividualChange<T>> changes;
		try {
			changes = createChanges(entities, updater);
		} catch(Throwable t) {
			throw new BlueDbException("Error updating values", t);
		}

		Collections.sort(changes);
		PendingBatchChange<T> change = PendingBatchChange.createBatchChange(changes);

		RecoveryManager<T> recoveryManager = collection.getRecoveryManager();
		recoveryManager.saveChange(change);
		change.apply(collection);
		recoveryManager.markComplete(change);
	}

	protected List<IndividualChange<T>> createChanges(List<BlueEntity<T>> entities, Updater<T> updater) {
		List<IndividualChange<T>> updates = new ArrayList<>();
		for (BlueEntity<T> entity: entities) {
			try {
				IndividualChange<T> update = createChange(entity, updater);
				updates.add(update);
			} catch(SerializationException e) {
				new BlueDbException("Failed to clone objects that will be updated by a query. This object will be skipped. Key: " + entity.getKey(), e);
			}
		}
		return updates;
	}

	private IndividualChange<T> createChange(BlueEntity<T> entity, Updater<T> updater) throws SerializationException {
		BlueSerializer serializer = collection.getSerializer();
		BlueKey key = entity.getKey();
		T oldValue = serializer.clone(entity.getValue());
		T newValue = serializer.clone(oldValue);
		updater.update(newValue);
		return new IndividualChange<T>(key, oldValue, newValue);
	}

	@Override
	public String toString() {
		return "<UpdateMultipleTask on query " + query.toString() + ">";
	}
}
