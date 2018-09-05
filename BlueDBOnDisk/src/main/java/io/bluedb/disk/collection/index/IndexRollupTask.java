package io.bluedb.disk.collection.index;

import java.io.Serializable;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.recovery.PendingIndexRollup;
import io.bluedb.disk.recovery.Recoverable;
import io.bluedb.disk.recovery.RecoveryManager;
import io.bluedb.disk.segment.rollup.IndexRollupTarget;

public class IndexRollupTask<T extends Serializable> implements Runnable {

	private final BlueCollectionOnDisk<T> collection;
	private final IndexRollupTarget rollupTarget;
	
	public IndexRollupTask(BlueCollectionOnDisk<T> collection, IndexRollupTarget rollupTarget) {
		this.collection = collection;
		this.rollupTarget = rollupTarget;
	}

	@Override
	public void run() {
		RecoveryManager<T> recoveryManager = collection.getRecoveryManager();
		String indexName = rollupTarget.getIndexName();
		Recoverable<T> change = new PendingIndexRollup<>(indexName, rollupTarget);
		try {
			recoveryManager.saveChange(change);
			change.apply(collection);
			recoveryManager.markComplete(change);
		} catch (BlueDbException e) {
			e.printStackTrace();
		}
	}
}
