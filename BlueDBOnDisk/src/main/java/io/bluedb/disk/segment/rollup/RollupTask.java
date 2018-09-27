package io.bluedb.disk.segment.rollup;

import java.io.Serializable;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.recovery.PendingRollup;
import io.bluedb.disk.recovery.Recoverable;
import io.bluedb.disk.recovery.RecoveryManager;

public class RollupTask<T extends Serializable> implements Runnable {

	private final BlueCollectionOnDisk<T> collection;
	private final RollupTarget rollupTarget;
	
	public RollupTask(BlueCollectionOnDisk<T> collection, RollupTarget rollupTarget) {
		this.collection = collection;
		this.rollupTarget = rollupTarget;
	}

	public RollupTarget getTarget() {
		return rollupTarget;
	}

	@Override
	public void run() {
		RecoveryManager<T> recoveryManager = collection.getRecoveryManager();
		Recoverable<T> change = new PendingRollup<>(rollupTarget);
		try {
			recoveryManager.saveChange(change);
			change.apply(collection);
			recoveryManager.markComplete(change);
		} catch (BlueDbException e) {
			e.printStackTrace();
		}
	}

	@Override
	public String toString() {
		return "RollupTask [@" + rollupTarget.getSegmentGroupingNumber() + ", " + rollupTarget.getRange().getStart() + "_" + rollupTarget.getRange().getEnd() + "]";
	}
}
