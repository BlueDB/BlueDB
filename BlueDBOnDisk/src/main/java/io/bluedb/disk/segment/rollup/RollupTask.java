package io.bluedb.disk.segment.rollup;

import java.io.Serializable;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.recovery.PendingRollup;
import io.bluedb.disk.recovery.Recoverable;
import io.bluedb.disk.recovery.RecoveryManager;
import io.bluedb.disk.segment.Range;

public class RollupTask<T extends Serializable> implements Runnable {

	private final BlueCollectionOnDisk<T> collection;
	private final Range timeRange;
	
	public RollupTask(BlueCollectionOnDisk<T> collection, Range timeRange) {
		this.collection = collection;
		this.timeRange = timeRange;
	}

	@Override
	public void run() {
		RecoveryManager<T> recoveryManager = collection.getRecoveryManager();
		Recoverable<T> change = new PendingRollup<>(timeRange);
		try {
			recoveryManager.saveChange(change);
			change.apply(collection);
			recoveryManager.removeChange(change);
		} catch (BlueDbException e) {
			e.printStackTrace();
		}
	}
}
