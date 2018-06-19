package io.bluedb.disk.collection;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
import io.bluedb.disk.Blutils;
import io.bluedb.disk.LockManager;
import io.bluedb.disk.query.BlueQueryImpl;
import io.bluedb.disk.recovery.PendingChange;
import io.bluedb.disk.recovery.RecoveryManager;
import io.bluedb.disk.segment.BlueEntity;
import io.bluedb.disk.segment.Segment;
import io.bluedb.disk.segment.SegmentIdConverter;

public class BlueCollectionImpl<T extends Serializable> implements BlueCollection<T> {

	ExecutorService executor = Executors.newFixedThreadPool(1);

	private Class<T> type;
//	private LockManager locks = new LockManager();
	private RecoveryManager recoveryManager = new RecoveryManager();

	public BlueCollectionImpl(Class<T> type) {
		this.type = type;
	}

	@Override
	public void insert(BlueKey key, T value) throws BlueDbException {
		if (contains(key)) {
			throw new DuplicateKeyException("key already exists: " + key);
		}
		Runnable updateTask = new Runnable(){
			@Override
			public void run() {
//				locks.lock(key);
				try {
					PendingChange change = PendingChange.createInsert(key, value);
					applyUpdateWithRecovery(key, change);
				} catch (Throwable t) {
					// TODO rollback or try again?
				} finally {
//					locks.unlock(key);
				}
			}
		};
		Future<?> future = executor.submit(updateTask);
		try {
			future.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			throw new BlueDbException("insert failed for key " + key.toString(), e);
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
		Runnable updateTask = new Runnable(){
			@Override
			public void run() {
				try {
					T value = get(key);
					PendingChange change = PendingChange.createUpdate(key, value, updater);
					applyUpdateWithRecovery(key, change);
				} catch (BlueDbException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};
		Future<?> future = executor.submit(updateTask);
		try {
			future.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			throw new BlueDbException("update failed for key " + key.toString(), e);
		}
		
//		locks.lock(key);
//		try {
//			T value = get(key);
//			PendingChange change = PendingChange.createUpdate(key, value, updater);
//			applyUpdateWithRecovery(key, change);
//		} catch (Throwable t) {
//			// TODO rollback or try again?
//		} finally {
//			locks.unlock(key);
//		}
	}

	@Override
	public void delete(BlueKey key) throws BlueDbException {
		Runnable deleteTask = new Runnable(){
			@Override
			public void run() {
		//		locks.lock(key);
				try {
					PendingChange change = PendingChange.createDelete(key);
					applyUpdateWithRecovery(key, change);
				} catch (Throwable t) {
					// TODO rollback or try again?
				} finally {
		//			locks.unlock(key);
				}
			}
		};
		Future<?> future = executor.submit(deleteTask);
		try {
			future.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			throw new BlueDbException("delete failed for key " + key.toString(), e);
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
		Runnable updateTask = new Runnable(){
			@Override
			public void run() {
				try {
					for (BlueEntity entity: findMatches(minTime, maxTime, conditions)) {
						BlueKey key = entity.getKey();
						T value = (T) entity.getObject();
						PendingChange change = PendingChange.createDelete(key);
						applyUpdateWithRecovery(key, change);
//						update(entity.getKey(), updater);
					}
				} catch (BlueDbException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};
		Future<?> future = executor.submit(updateTask);
		try {
			future.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			throw new BlueDbException("delete query failed", e);
		}
//		for (BlueEntity entity: findMatches(minTime, maxTime, conditions)) {
//			delete(entity.getKey());
//		}
	}

	public void updateAll(long minTime, long maxTime, List<Condition<T>> conditions, Updater<T> updater) throws BlueDbException {
		Runnable updateTask = new Runnable(){
			@Override
			public void run() {
				try {
					for (BlueEntity entity: findMatches(minTime, maxTime, conditions)) {
						BlueKey key = entity.getKey();
						T value = (T) entity.getObject();
						PendingChange change = PendingChange.createUpdate(key, value, updater);
						applyUpdateWithRecovery(key, change);
//						update(entity.getKey(), updater);
					}
				} catch (BlueDbException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};
		Future<?> future = executor.submit(updateTask);
		try {
			future.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			throw new BlueDbException("update query failed", e);
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
		List<File> segmentFiles = Blutils.listFiles(".", ".segment");
		for (File segmentFile: segmentFiles) {
			String fileName = segmentFile.getName();
			String segmentIdStr = fileName.substring(0, fileName.indexOf(".segment"));
			long segmentId = Long.parseLong(segmentIdStr);
			if (segmentId >= minSegmentId && segmentId <= maxSegmentId) {
				segments.add(new Segment(segmentIdStr));
			}
		}
		return segments;
	}

	public String getPath() {
		// TODO
		return null;
	}
	
	public void shutdown() {
		// TODO shutdown executors? what else?
	}

	private void applyUpdateWithRecovery(BlueKey key, PendingChange change) throws BlueDbException {
		recoveryManager.saveChange(change);
		List<Segment> segments = getSegments(key);
		for (Segment segment: segments) {
			change.applyChange(segment);
		}
		recoveryManager.removeChange(change);
	}
}
