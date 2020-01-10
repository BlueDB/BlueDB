package org.bluedb.disk.segment.rollup;

import java.io.Serializable;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.collection.ReadOnlyBlueCollectionOnDisk;
import org.bluedb.disk.recovery.PendingRollup;
import org.bluedb.disk.recovery.Recoverable;
import org.bluedb.disk.recovery.RecoveryManager;

public class RollupTask<T extends Serializable> implements Runnable {

	private final ReadOnlyBlueCollectionOnDisk<T> collection;
	private final RollupTarget rollupTarget;
	
	public RollupTask(ReadOnlyBlueCollectionOnDisk<T> collection, RollupTarget rollupTarget) {
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
