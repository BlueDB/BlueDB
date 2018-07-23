package io.bluedb.disk.segment.rollup;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.segment.Range;

public class RollupScheduler implements Runnable {

	private static final long WAIT_BETWEEN_REVIEWS_DEFAULT = 30_000;
	private static final long WAIT_BEFORE_ROLLUP = 3600_000;

	private long waitBetweenReviews = WAIT_BETWEEN_REVIEWS_DEFAULT;
	private final BlueCollectionOnDisk<?> collection;
	private final Map<Range, Long> lastInsertTimes;
	private Thread thread;
	private boolean isStopped;

	public RollupScheduler(BlueCollectionOnDisk<?> collection) {
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
			isStopped |= !Blutils.trySleep(waitBetweenReviews);
		}
	}

	public void stop() {
		isStopped = true;
	}

	public void reportInsert(Range timeRange) {
		reportInsert(timeRange, System.currentTimeMillis());
	}

	public void reportInsert(Range timeRange, long timeMillis) {
		if (getLastInsertTime(timeRange) < timeMillis) {
			lastInsertTimes.put(timeRange, timeMillis);
		}
	}

	public long getLastInsertTime(Range timeRange) {
		return lastInsertTimes.getOrDefault(timeRange, Long.MIN_VALUE);
	}

	public boolean isRunning() {
		return !isStopped;
	}

	protected static boolean isReadyForRollup(long lastInsert) {
		long now = System.currentTimeMillis();
		return (now - lastInsert) > WAIT_BEFORE_ROLLUP;
	}

	protected void scheduleReadyRollups() {
		for (Range timeRange: timeRangesReadyForRollup()) {
			scheduleRollup(timeRange);
		}
	}

	public void setWaitBetweenReviews(long newWaitTimeMillis) {
		waitBetweenReviews = newWaitTimeMillis;
	}

	protected List<Range> timeRangesReadyForRollup() {
		List<Range> results = new ArrayList<>();
		for (Entry<Range, Long> entry: lastInsertTimes.entrySet()) {
			if (isReadyForRollup(entry.getValue())) {
				results.add(entry.getKey());
			}
		}
		return results;
	}

	private void scheduleRollup(Range timeRange) {
		collection.scheduleRollup(timeRange);
		lastInsertTimes.remove(timeRange);
	}
}
