package org.bluedb.disk.collection.index;

import java.io.Serializable;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.recovery.PendingIndexRollup;
import org.bluedb.disk.recovery.Recoverable;
import org.bluedb.disk.recovery.RecoveryManager;
import org.bluedb.disk.segment.rollup.IndexRollupTarget;

public class IndexRollupTask<T extends Serializable> implements Runnable {

	private final ReadWriteCollectionOnDisk<T> collection;
	private final IndexRollupTarget rollupTarget;
	
	public IndexRollupTask(ReadWriteCollectionOnDisk<T> collection, IndexRollupTarget rollupTarget) {
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

	@Override
	public String toString() {
		return "IndexRollupTask [" + rollupTarget.getIndexName()+ "@" + rollupTarget.getSegmentGroupingNumber() + ", " + rollupTarget.getRange().getStart() + "_" + rollupTarget.getRange().getEnd() + "]";
	}
}
