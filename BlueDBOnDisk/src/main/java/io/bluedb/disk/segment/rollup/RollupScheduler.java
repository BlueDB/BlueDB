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
	protected static final long WAIT_AFTER_WRITE_BEFORE_ROLLUP = 3600_000;
	protected static final long WAIT_AFTER_READ_BEFORE_ROLLUP = 60_000;

	private long waitBetweenReviews = WAIT_BETWEEN_REVIEWS_DEFAULT;
	private final BlueCollectionOnDisk<?> collection;
	private final Map<RollupTarget, Long> rollupTimes;

	private Thread thread;
	private boolean isStopped;

	public RollupScheduler(BlueCollectionOnDisk<?> collection) {
		rollupTimes = new ConcurrentHashMap<>();
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

	public void reportWrite(long segmentGroupingNumber, Range range) {
		RollupTarget target = new RollupTarget(segmentGroupingNumber, range);
		reportWrite(target, System.currentTimeMillis());
	}

	public void reportWrite(RollupTarget rollupTarget, long timeMillis) {
		long currentRollupTime = rollupTimes.getOrDefault(rollupTarget, Long.MIN_VALUE);
		long newRollupTime = Math.max(currentRollupTime, timeMillis + WAIT_AFTER_WRITE_BEFORE_ROLLUP);
		if (newRollupTime > currentRollupTime) {
			rollupTimes.put(rollupTarget, newRollupTime);
		}
	}

	public long getScheduledRollupTime(RollupTarget target) {
		return rollupTimes.getOrDefault(target, Long.MAX_VALUE);
	}

	public boolean isRunning() {
		return !isStopped;
	}

	protected void scheduleReadyRollups() {
		for (RollupTarget target: rollupTargetsReadyForRollup()) {
			scheduleRollup(target);
		}
	}

	public void forceScheduleRollups() {
		List<RollupTarget> allRangesWaitingForRollups = new ArrayList<>(rollupTimes.keySet());
		for (RollupTarget timeRange: allRangesWaitingForRollups) {
			scheduleRollup(timeRange);
		}
	}

	public void setWaitBetweenReviews(long newWaitTimeMillis) {
		waitBetweenReviews = newWaitTimeMillis;
	}

	protected List<RollupTarget> rollupTargetsReadyForRollup() {
		long now = System.currentTimeMillis();
		List<RollupTarget> results = new ArrayList<>();
		for (Entry<RollupTarget, Long> entry: rollupTimes.entrySet()) {
			if (entry.getValue() < now) {
				results.add(entry.getKey());
			}
		}
		return results;
	}

	private void scheduleRollup(RollupTarget target) {
		collection.scheduleRollup(target);
		rollupTimes.remove(target);
	}
}
