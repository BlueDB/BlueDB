package io.bluedb.disk.collection.index;

import java.io.Serializable;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.recovery.PendingIndexRollup;
import io.bluedb.disk.recovery.Recoverable;
import io.bluedb.disk.recovery.RecoveryManager;
import io.bluedb.disk.segment.rollup.RollupTarget;

public class IndexRollupTask<T extends Serializable> implements Runnable {

	private final BlueIndexOnDisk<?, T> index;
	private final RollupTarget rollupTarget;
	
	public IndexRollupTask(BlueIndexOnDisk<?, T> index, RollupTarget rollupTarget) {
		this.index = index;
		this.rollupTarget = rollupTarget;
	}

	@Override
	public void run() {
		BlueCollectionOnDisk<T> collection = index.getCollection();
		RecoveryManager<T> recoveryManager = index.getCollection().getRecoveryManager();
		String indexName = index.getName();
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
