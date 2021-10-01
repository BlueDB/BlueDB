package org.bluedb.disk.collection.task;

import java.io.Serializable;

import org.bluedb.api.Updater;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.query.QueryOnDisk;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.recovery.PendingMassChange;
import org.bluedb.disk.recovery.RecoveryManager;
import org.bluedb.disk.serialization.BlueEntity;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.validation.SerializationException;

public class UpdateMultipleTask<T extends Serializable> extends QueryTask {
	private final ReadWriteCollectionOnDisk<T> collection;
	private final QueryOnDisk<T> query;
	private final Updater<T> updater;


	public UpdateMultipleTask(ReadWriteCollectionOnDisk<T> collection, QueryOnDisk<T> query, Updater<T> updater) {
		this.collection = collection;
		this.query = query;
		this.updater = updater;
	}

	@Override
	public void execute() throws BlueDbException {
		RecoveryManager<T> recoveryManager = collection.getRecoveryManager();
		PendingMassChange<T> changeBatch = recoveryManager.saveMassChange(query, this::createChange);
		changeBatch.apply(collection);
		recoveryManager.markComplete(changeBatch);
	}

	protected IndividualChange<T> createChange(BlueEntity<T> entity) throws SerializationException {
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
