package org.bluedb.disk.collection.task;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.bluedb.api.Mapper;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.collection.BlueCollectionOnDisk;
import org.bluedb.disk.query.BlueQueryOnDisk;
import org.bluedb.disk.recovery.PendingChange;
import org.bluedb.disk.recovery.RecoveryManager;
import org.bluedb.disk.serialization.BlueEntity;
import org.bluedb.disk.serialization.BlueSerializer;

public class ReplaceMultipleTask<T extends Serializable> extends QueryTask {
	private final BlueCollectionOnDisk<T> collection;
	private final BlueQueryOnDisk<T> query;
	private final Mapper<T> mapper;


	public ReplaceMultipleTask(BlueCollectionOnDisk<T> collection, BlueQueryOnDisk<T> query, Mapper<T> mapper) {
		this.collection = collection;
		this.query = query;
		this.mapper = mapper;
	}

	@Override
	public void execute() throws BlueDbException {
		List<BlueEntity<T>> entities = query.getEntities();
		List<PendingChange<T>> updates;
		try {
			updates = createUpdates(entities, mapper);
		} catch(Throwable t) {
			t.printStackTrace();
			throw new BlueDbException("Error updating values", t);
		}

		RecoveryManager<T> recoveryManager = collection.getRecoveryManager();
		for (PendingChange<T> update: updates) {
			recoveryManager.saveChange(update);
			update.apply(collection);
			recoveryManager.markComplete(update);
		}
	}

	private List<PendingChange<T>> createUpdates(List<BlueEntity<T>> entities, Mapper<T> mapper) {
		BlueSerializer serializer = collection.getSerializer();

		List<PendingChange<T>> updates = new ArrayList<>();
		for (BlueEntity<T> entity: entities) {
			PendingChange<T> update = PendingChange.createUpdate(entity, mapper, serializer);
			updates.add(update);
		}
		return updates;
	}

	@Override
	public String toString() {
		return "<" + this.getClass().getSimpleName() + " on query " + query.toString() + ">";
	}
}
