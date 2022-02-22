package org.bluedb.disk.collection.task;

import java.io.Serializable;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.query.QueryOnDisk;
import org.bluedb.disk.recovery.EntityToChangeMapper;
import org.bluedb.disk.recovery.PendingMassChange;
import org.bluedb.disk.recovery.RecoveryManager;

public class BatchQueryChangeTask<T extends Serializable> extends QueryTask {
	private final ReadWriteCollectionOnDisk<T> collection;
	private final QueryOnDisk<T> query;
	private final EntityToChangeMapper<T> entityToChangeMapper;
	
	public BatchQueryChangeTask(String description, ReadWriteCollectionOnDisk<T> collection, QueryOnDisk<T> query, EntityToChangeMapper<T> entityToChangeMapper) {
		super(description);
		this.collection = collection;
		this.query = query;
		this.entityToChangeMapper = entityToChangeMapper;
	}

	@Override
	public void execute() throws BlueDbException {
		RecoveryManager<T> recoveryManager = collection.getRecoveryManager();
		PendingMassChange<T> changeBatch = recoveryManager.saveMassChangeForQueryChange(query, entityToChangeMapper);
		changeBatch.apply(collection);
		recoveryManager.markComplete(changeBatch);
	}
}
