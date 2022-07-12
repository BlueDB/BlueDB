package org.bluedb.disk.collection.task;

import java.io.Serializable;
import java.util.Iterator;

import org.bluedb.api.datastructures.BlueKeyValuePair;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.serialization.BlueEntity;

public class BatchUpsertEntitiesTask<T extends Serializable> extends BatchUpsertChangeTask<T> {

	public BatchUpsertEntitiesTask(String description, ReadWriteCollectionOnDisk<T> collection, Iterator<BlueKeyValuePair<T>> keyValuePairIterator) {
		super(description, collection, keyValuePairIterator);
	}

	@Override
	protected IndividualChange<T> createUpdateChange(BlueEntity<T> oldEntity, BlueEntity<T> newEntity) throws BlueDbException {
		return IndividualChange.createUpdateKeyAndValueChange(oldEntity, newEntity);
	}

}
