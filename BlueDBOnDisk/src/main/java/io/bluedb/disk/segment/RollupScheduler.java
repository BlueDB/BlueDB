package io.bluedb.disk.segment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.collection.BlueCollectionImpl;

public class RollupScheduler implements Runnable {

	private static final long WAIT_BETWEEN_REVIEWS = 30_000; // TODO something more sophisticated than just wait ?
	private static final long WAIT_BEFORE_ROLLUP = 3600_000; // TODO something more sophisticated?

	private final BlueCollectionImpl<?> collection;
	private final Map<TimeRange, Long> lastInsertTimes;
	private Thread thread;
	private boolean isStopped;

	public RollupScheduler(BlueCollectionImpl<?> collection) {
		lastInsertTimes = new ConcurrentHashMap<>();
		this.collection = collection;
	}

	public void start() {
		isStopped = false;
		thread = new Thread(this, "RollupScheduler");
		thread.start();
	}

	@Override
	public void run() {
		while (!isStopped) {
			scheduleReadyRollups();
			isStopped |= !Blutils.trySleep(WAIT_BETWEEN_REVIEWS);
		}
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

	protected static boolean isReadyForRollup(long lastInsert) {
		long now = System.currentTimeMillis();
		return (now - lastInsert) > WAIT_BEFORE_ROLLUP;
	}

	protected void scheduleReadyRollups() {
		for (TimeRange timeRange: timeRangesReadyForRollup()) {
			scheduleRollup(timeRange);
		}
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

	private void scheduleRollup(TimeRange timeRange) {
		collection.scheduleRollup(timeRange);
		lastInsertTimes.remove(timeRange);
	}
}
