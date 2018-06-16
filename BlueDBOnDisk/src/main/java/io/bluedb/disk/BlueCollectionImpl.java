package io.bluedb.disk;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import io.bluedb.api.BlueCollection;
import io.bluedb.api.BlueQuery;
import io.bluedb.api.Condition;
import io.bluedb.api.Updater;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.exceptions.DuplicateKeyException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.recovery.PendingChange;
import io.bluedb.disk.recovery.RecoveryManager;

public class BlueCollectionImpl<T extends Serializable> implements BlueCollection<T> {

	private Class<T> type;
	private LockManager locks = new LockManager();
	private RecoveryManager recoveryManager = new RecoveryManager();

	public BlueCollectionImpl(Class<T> type) {
		this.type = type;
	}

	@Override
	public void insert(BlueKey key, T value) throws BlueDbException {
		if (contains(key)) {
			throw new DuplicateKeyException("key already exists: " + key);
		}
		locks.lock(key);
		try {
			PendingChange change = PendingChange.createInsert(key, value);
			applyUpdateWithRecovery(key, change);
		} catch (Throwable t) {
			// TODO rollback or try again?
		} finally {
			locks.unlock(key);
		}
	}

	@Override
	public T get(BlueKey key) throws BlueDbException {
		List<Segment> segments = getSegments(key);
		List<BlueEntity> entitiesInSegment = segments.get(0).read();
		for (BlueEntity entity: entitiesInSegment) {
			if (entity.getKey().equals(key)) {
				return (T) entity.getObject();
			}
		}
		return null;
	}

	@Override
	public void update(BlueKey key, Updater<T> updater) throws BlueDbException {
		locks.lock(key);
		try {
			T value = get(key);
			PendingChange change = PendingChange.createUpdate(key, value, updater);
			applyUpdateWithRecovery(key, change);
		} catch (Throwable t) {
			// TODO rollback or try again?
		} finally {
			locks.unlock(key);
		}
	}

	private void applyUpdateWithRecovery(BlueKey key, PendingChange change) throws BlueDbException {
		recoveryManager.saveChange(change);
		List<Segment> segments = getSegments(key);
		for (Segment segment: segments) {
			change.applyChange(segment);
		}
		recoveryManager.removeChange(change);
	}

	@Override
	public void delete(BlueKey key) throws BlueDbException {
		locks.lock(key);
		try {
			PendingChange change = PendingChange.createDelete(key);
			applyUpdateWithRecovery(key, change);
		} catch (Throwable t) {
			// TODO rollback or try again?
		} finally {
			locks.unlock(key);
		}
	}

	@Override
	public BlueQuery<T> query() {
		return new BlueQueryImpl<T>(this);
	}

	public List<T> getList(long minTime, long maxTime, List<Condition<T>> conditions) throws BlueDbException {
		 return (List<T>) findMatches(minTime, maxTime, conditions).stream().map((e) -> e.getObject()).collect(Collectors.toList());
	}

	public void deleteAll(long minTime, long maxTime, List<Condition<T>> conditions) throws BlueDbException {
		for (BlueEntity entity: findMatches(minTime, maxTime, conditions)) {
			delete(entity.getKey());
		}
	}

	public void updateAll(long minTime, long maxTime, List<Condition<T>> conditions, Updater<T> updater) throws BlueDbException {
		for (BlueEntity entity: findMatches(minTime, maxTime, conditions)) {
			update(entity.getKey(), updater);
		}
	}

	private List<BlueEntity> findMatches(long minTime, long maxTime, List<Condition<T>> conditions) throws BlueDbException {
		List<BlueEntity> results = new ArrayList<>();
		List<Segment> segments = getSegments(minTime, maxTime);
		for (Segment segment: segments) {
			List<BlueEntity> entitesInSegment = segment.read();
			for (BlueEntity entity: entitesInSegment) {
				T value = (T)entity.getObject();
				if(meetsConditions(conditions, value) && meetsTimeConstraint(entity.getKey(), minTime, maxTime)) {
					results.add(entity);
				}
			}
		}
		return results;
	}

	private <X extends Serializable> boolean meetsConditions(List<Condition<X>> conditions, X object) {
		for (Condition<X> condition: conditions) {
			if (!condition.test(object)) {
				return false;
			}
		}
		return true;
	}

	private boolean meetsTimeConstraint(BlueKey key, long minTime, long maxTime) {
		if (key instanceof TimeFrameKey) {
			TimeFrameKey timeKey = (TimeFrameKey) key;
			return timeKey.getEndTime() >= minTime && timeKey.getStartTime() <= maxTime;
		}
		if (key instanceof TimeKey) {
			TimeKey timeKey = (TimeKey) key;
			return timeKey.getTime() >= minTime && timeKey.getTime() <= maxTime;
		}
		return true;
	}
	@Override
	public boolean contains(BlueKey key) throws BlueDbException {
		return get(key) != null;
	}

	private List<Segment> getSegments(BlueKey key) {
		List<Segment> segments = new ArrayList<>();
		if (key instanceof TimeFrameKey) {
			TimeFrameKey timeFrameKey = (TimeFrameKey)key;
			for (Long l: SegmentIdConverter.getSegments(timeFrameKey.getStartTime(), timeFrameKey.getEndTime())) {
				segments.add(new Segment(String.valueOf(l)));
			}
		} else if (key instanceof TimeKey) {
			TimeKey timeKey = (TimeKey)key;
			long segmentId = SegmentIdConverter.convertTimeToSegmentId(timeKey.getTime());
			segments.add(new Segment(String.valueOf(segmentId)));
		} else {
			segments.add(new Segment(key.toString())); // TODO break into safely named segments
		}
		return segments;
	}

	private List<Segment> getSegments(long minTime, long maxTime) {
		long minSegmentId = SegmentIdConverter.convertTimeToSegmentId(minTime);
		long maxSegmentId = SegmentIdConverter.convertTimeToSegmentId(maxTime);
		List<Segment> segments = new ArrayList<>();
		// TODO this should be way better
		File folder = new File(".");
		File[] filesInFolder = folder.listFiles();
		for (File file: filesInFolder)
			if(file.getName().endsWith(".segment")) {
				String fileName = file.getName();
				String segmentIdStr = fileName.substring(0, fileName.indexOf(".segment"));
				long segmentId = Long.parseLong(segmentIdStr);
				if (segmentId >= minSegmentId && segmentId <= maxSegmentId) {
					segments.add(new Segment(segmentIdStr));
				}
			}
		return segments;
	}
}
