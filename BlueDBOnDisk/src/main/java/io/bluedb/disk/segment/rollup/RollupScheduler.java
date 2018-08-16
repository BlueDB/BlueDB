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
	private final Map<RollupTarget, Long> lastInsertTimes;
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

	public void reportInsert(long segmentGroupingNumber, Range range) {
		RollupTarget target = new RollupTarget(segmentGroupingNumber, range);
		reportInsert(target, System.currentTimeMillis());
	}

	public void reportInsert(RollupTarget rollupTarget, long timeMillis) {
		if (getLastInsertTime(rollupTarget) < timeMillis) {
			lastInsertTimes.put(rollupTarget, timeMillis);
		}
	}

	public long getLastInsertTime(RollupTarget target) {
		return lastInsertTimes.getOrDefault(target, Long.MIN_VALUE);
	}

	public boolean isRunning() {
		return !isStopped;
	}

	protected static boolean isReadyForRollup(long lastInsert) {
		long now = System.currentTimeMillis();
		return (now - lastInsert) > WAIT_BEFORE_ROLLUP;
	}

	protected void scheduleReadyRollups() {
		for (RollupTarget target: rollupTargetsReadyForRollup()) {
			scheduleRollup(target);
		}
	}

	public void forceScheduleRollups() {
		List<RollupTarget> allRangesWaitingForRollups = new ArrayList<>(lastInsertTimes.keySet());
		for (RollupTarget timeRange: allRangesWaitingForRollups) {
			scheduleRollup(timeRange);
		}
	}

	public void setWaitBetweenReviews(long newWaitTimeMillis) {
		waitBetweenReviews = newWaitTimeMillis;
	}

	protected List<RollupTarget> rollupTargetsReadyForRollup() {
		List<RollupTarget> results = new ArrayList<>();
		for (Entry<RollupTarget, Long> entry: lastInsertTimes.entrySet()) {
			if (isReadyForRollup(entry.getValue())) {
				results.add(entry.getKey());
			}
		}
		return results;
	}

	private void scheduleRollup(RollupTarget target) {
		collection.scheduleRollup(target);
		lastInsertTimes.remove(target);
	}
}
