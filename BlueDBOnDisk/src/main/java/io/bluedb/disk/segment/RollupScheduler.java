package io.bluedb.disk.segment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import io.bluedb.disk.collection.BlueCollectionImpl;

public class RollupScheduler {

	private static final long WAIT_BETWEEN_REVIEWS = 30_000; // TODO something more sophisticated than just wait ?
	private static final long WAIT_BEFORE_ROLLUP = 3600_000; // TODO something more sophisticated?

	private final BlueCollectionImpl<?> collection;
	private final Map<TimeRange, Long> lastInsertTimes;
	private boolean isStopped = false;

	public RollupScheduler(BlueCollectionImpl<?> collection) {
		lastInsertTimes = new ConcurrentHashMap<>();
		this.collection = collection;
	}

	public void start() {
		Runnable runnable = new Runnable(){
			@Override
			public void run() {
				while (!isStopped) {
					for (TimeRange timeRange: timeRangesReadyForRollup()) {
						scheduleRollup(timeRange);
					}
					trySleep(WAIT_BETWEEN_REVIEWS);
				}
			}
		};
		Thread thread = new Thread(runnable, "RollupScheduler");
		thread.start();
	}

	public void stop() {
		isStopped = true;
	}

	public void reportInsert(TimeRange timeRange) {
		reportInsert(timeRange, System.currentTimeMillis());
	}

	public void reportInsert(TimeRange timeRange, long timeMillis) {
		if (getLastInsertTime(timeRange) < timeMillis) {
			lastInsertTimes.put(timeRange, timeMillis);
		}
	}

	public long getLastInsertTime(TimeRange timeRange) {
		return lastInsertTimes.getOrDefault(timeRange, Long.MIN_VALUE);
	}

	private boolean isReadyForRollup(long lastInsert) {
		long now = System.currentTimeMillis();
		return (now - lastInsert) > WAIT_BEFORE_ROLLUP;
	}

	protected List<TimeRange> timeRangesReadyForRollup() {
		List<TimeRange> results = new ArrayList<>();
		for (Entry<TimeRange, Long> entry: lastInsertTimes.entrySet()) {
			if (isReadyForRollup(entry.getValue())) {
				results.add(entry.getKey());
			}
		}
		return results;
	}

	protected static void trySleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void scheduleRollup(TimeRange timeRange) {
		collection.scheduleRollup(timeRange);
		lastInsertTimes.remove(timeRange);
	}
}
