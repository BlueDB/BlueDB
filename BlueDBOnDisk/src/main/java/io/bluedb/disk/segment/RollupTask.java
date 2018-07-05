package io.bluedb.disk.segment;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.collection.BlueCollectionImpl;

public class RollupTask implements Runnable {

	private final BlueCollectionImpl<?> collection;
	private final TimeRange timeRange;
	
	public RollupTask(BlueCollectionImpl<?> collection, TimeRange timeRange) {
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
