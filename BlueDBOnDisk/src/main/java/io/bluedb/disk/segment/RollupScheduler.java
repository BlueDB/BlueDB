package io.bluedb.disk.segment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.collection.BlueCollectionImpl;

public class RollupScheduler {

	private static final long WAIT_BETWEEN_REVIEWS = 3_000; // TODO something more sophisticated than just wait ?
	private static final long WAIT_BEFORE_ROLLUP = 3_000; // TODO 

	private final BlueCollectionImpl<?> collection;
	private final Map<TimeRange, Long> lastInsertTimes;
	private boolean isStopped = false;

	public RollupScheduler(BlueCollectionImpl<?> collection) {
		lastInsertTimes = new ConcurrentHashMap<>();
		this.collection = collection;
		start();
	}

	public void stop() {
		isStopped = true;
	}

	public void reportInsert(TimeRange timeRange) {
		System.out.println("RollupScheduler.reportInsert(" + timeRange + ")");
		lastInsertTimes.put(timeRange, System.currentTimeMillis());
	}

	private boolean isReadyForRollup(long lastInsert) {
		long now = System.currentTimeMillis();
		return (now - lastInsert) > WAIT_BEFORE_ROLLUP;
	}

	private List<TimeRange> timeRangesReadyForRollup() {
		List<TimeRange> results = new ArrayList<>();
		for (Entry<TimeRange, Long> entry: lastInsertTimes.entrySet()) {
			if (isReadyForRollup(entry.getValue())) {
				results.add(entry.getKey());
			}
		}
		return results;
	}

	private void start() {
		System.out.println("starting RollupScheduler");
		Runnable runnable = new Runnable(){
			@Override
			public void run() {
				while (!isStopped) {
					try {
						for (TimeRange timeRange: timeRangesReadyForRollup()) {
							System.out.println("RollupScheduler scheduling rollup for " + timeRange);
							collection.scheduleRollup(timeRange);
							lastInsertTimes.remove(timeRange);
						}
						System.out.println("RollupScheduler reviewed");
						Thread.sleep(WAIT_BETWEEN_REVIEWS);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		};
		Thread thread = new Thread(runnable);
		thread.start();
	}
}
