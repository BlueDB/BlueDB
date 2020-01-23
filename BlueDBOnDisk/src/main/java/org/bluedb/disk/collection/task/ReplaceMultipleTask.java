package org.bluedb.disk.collection.task;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bluedb.api.Mapper;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.query.QueryOnDisk;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.recovery.PendingBatchChange;
import org.bluedb.disk.recovery.RecoveryManager;
import org.bluedb.disk.serialization.BlueEntity;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.validation.SerializationException;

public class ReplaceMultipleTask<T extends Serializable> extends QueryTask {
	private final ReadWriteCollectionOnDisk<T> collection;
	private final QueryOnDisk<T> query;
	private final Mapper<T> mapper;


	public ReplaceMultipleTask(ReadWriteCollectionOnDisk<T> collection, QueryOnDisk<T> query, Mapper<T> mapper) {
		this.collection = collection;
		this.query = query;
		this.mapper = mapper;
	}

	@Override
	public void execute() throws BlueDbException {
		List<BlueEntity<T>> entities = query.getEntities();
		List<IndividualChange<T>> changes;
		try {
			changes = createChanges(entities, mapper);
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

	protected List<IndividualChange<T>> createChanges(List<BlueEntity<T>> entities, Mapper<T> mapper) {
		List<IndividualChange<T>> updates = new ArrayList<>();
		for (BlueEntity<T> entity: entities) {
			try {
				IndividualChange<T> update = createChange(entity, mapper);
				updates.add(update);
			} catch(SerializationException e) {
				new BlueDbException("Failed to clone objects that will be replaced by a query. This object will be skipped. Key: " + entity.getKey(), e);
			}
		}
		return updates;
	}
	
	private IndividualChange<T> createChange(BlueEntity<T> entity, Mapper<T> mapper) throws SerializationException {
		BlueSerializer serializer = collection.getSerializer();
		BlueKey key = entity.getKey();
		T oldValue = serializer.clone(entity.getValue());
		T newValue = mapper.update(serializer.clone(oldValue));
		return new IndividualChange<T>(key, oldValue, newValue);
	}

	@Override
	public String toString() {
		return "<" + this.getClass().getSimpleName() + " on query " + query.toString() + ">";
	}
}
