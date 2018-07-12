package io.bluedb.disk.segment.rollup;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.segment.Range;

public class RollupTask implements Runnable {

	private final BlueCollectionOnDisk<?> collection;
	private final Range timeRange;
	
	public RollupTask(BlueCollectionOnDisk<?> collection, Range timeRange) {
		this.collection = collection;
		this.timeRange = timeRange;
	}

	@Override
	public void run() {
		try {
			collection.rollup(timeRange);
		} catch (BlueDbException e) {
			e.printStackTrace();
		}
	}
}
