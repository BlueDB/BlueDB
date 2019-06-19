package org.bluedb.disk.segment.rollup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.bluedb.disk.collection.BlueCollectionOnDisk;
import org.bluedb.disk.collection.index.IndexRollupTask;

public class RollupScheduler {

	private static final long WAIT_BETWEEN_REVIEWS_DEFAULT = 30_000;

	private long waitBetweenReviews = WAIT_BETWEEN_REVIEWS_DEFAULT;
	private final BlueCollectionOnDisk<?> collection;
	private final Map<RollupTarget, Long> rollupTimes;

	public RollupScheduler(BlueCollectionOnDisk<?> collection) {
		rollupTimes = new ConcurrentHashMap<>();
		this.collection = collection;
	}

	public void start() {
		TimerTask task = new TimerTask() {
			@Override
			public void run() {
				scheduleLimitedReadyRollups();
			}
		};
		collection.getSharedExecutor().scheduleAtFixedRate(task, waitBetweenReviews, waitBetweenReviews, TimeUnit.MILLISECONDS);
	}

	public void reportReads(List<? extends RollupTarget> rollupTargets) {
		long timeMillis = System.currentTimeMillis();
		for (RollupTarget target: rollupTargets) {
			reportRead(target, timeMillis);
		}
	}

	public void reportWrites(List< ? extends RollupTarget> rollupTargets) {
		long timeMillis = System.currentTimeMillis();
		for (RollupTarget target: rollupTargets) {
			reportWrite(target, timeMillis);
		}
	}

	public void reportRead(RollupTarget rollupTarget, long timeMillis) {
		setRollupTime(rollupTarget, timeMillis + rollupTarget.getWriteRollupDelay());
	}

	public void reportWrite(RollupTarget rollupTarget, long timeMillis) {
		setRollupTime(rollupTarget, timeMillis + rollupTarget.getWriteRollupDelay());
	}

	public void setRollupTime(RollupTarget rollupTarget, long timeMillis) {
		long currentRollupTime = rollupTimes.getOrDefault(rollupTarget, Long.MIN_VALUE);
		long newRollupTime = Math.max(currentRollupTime, timeMillis);
		if (newRollupTime > currentRollupTime) {
			rollupTimes.put(rollupTarget, newRollupTime);
		}
	}

	public long getScheduledRollupTime(RollupTarget target) {
		return rollupTimes.getOrDefault(target, Long.MAX_VALUE);
	}

	protected void scheduleLimitedReadyRollups() {
		int rollupsToSchedule = 30 - collection.getQueuedTaskCount();
		if (rollupsToSchedule > 0) {
			scheduleReadyRollups(rollupsToSchedule);
		}
	}

	protected void scheduleReadyRollups(int maxRollupsToSchedule) {
		for (RollupTarget target: rollupTargetsReadyForRollup(maxRollupsToSchedule)) {
			scheduleRollup(target);
		}
	}

	public void forceScheduleRollups() {
		List<RollupTarget> allRangesWaitingForRollups = new ArrayList<>(rollupTimes.keySet());
		for (RollupTarget timeRange: allRangesWaitingForRollups) {
			scheduleRollup(timeRange);
		}
	}

	protected List<RollupTarget> rollupTargetsReadyForRollup(int maxCount) {
		List<RollupTarget> readyRollups = rollupTargetsReadyForRollup();
		int toIndex = Math.min(maxCount, readyRollups.size());
		return readyRollups.subList(0, toIndex);  // TODO sort in some reasonable way
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

	protected void scheduleRollup(RollupTarget target) {
		Runnable rollupRunnable;
		if (target instanceof IndexRollupTarget) {
			IndexRollupTarget indexTarget = (IndexRollupTarget) target;
			rollupRunnable = new IndexRollupTask<>(collection, indexTarget);
		} else {
			rollupRunnable = new RollupTask<>(collection, target);
		}
		collection.submitTask(rollupRunnable);
		rollupTimes.remove(target);
	}

	public Map<RollupTarget, Long> getRollupTimes() {
		return new HashMap<>(rollupTimes);
	}
}
