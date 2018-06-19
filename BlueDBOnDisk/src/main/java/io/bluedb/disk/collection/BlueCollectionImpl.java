package io.bluedb.disk.collection;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
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
import io.bluedb.disk.BlueDbOnDisk;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.collection.task.DeleteMultipleTask;
import io.bluedb.disk.collection.task.DeleteTask;
import io.bluedb.disk.collection.task.InsertTask;
import io.bluedb.disk.collection.task.UpdateMultipleTask;
import io.bluedb.disk.collection.task.UpdateTask;
import io.bluedb.disk.query.BlueQueryImpl;
import io.bluedb.disk.recovery.PendingChange;
import io.bluedb.disk.recovery.RecoveryManager;
import io.bluedb.disk.segment.BlueEntity;
import io.bluedb.disk.segment.Segment;
import io.bluedb.disk.segment.SegmentIdConverter;

public class BlueCollectionImpl<T extends Serializable> implements BlueCollection<T> {

	ExecutorService executor = Executors.newFixedThreadPool(1);

	private Class<T> type;
	private final RecoveryManager recoveryManager;
	final private Path path;

	public BlueCollectionImpl(BlueDbOnDisk db, Class<T> type) {
		this.type = type;
		path = Paths.get(db.getPath().toString(), type.getName());
		path.toFile().mkdirs();
		recoveryManager = new RecoveryManager(this);
		recoveryManager.recover();
	}

	@Override
	public void insert(BlueKey key, T value) throws BlueDbException {
		Runnable insertTask = new InsertTask(recoveryManager, this, key, value);
		Future<?> future = executor.submit(insertTask);
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
		Runnable updateTask = new UpdateTask(recoveryManager, this, key, updater);
		Future<?> future = executor.submit(updateTask);
		try {
			future.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			throw new BlueDbException("update failed for key " + key.toString(), e);
		}
	}

	@Override
	public void delete(BlueKey key) throws BlueDbException {
		Runnable deleteTask = new DeleteTask(recoveryManager, this, key);
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
		List<BlueKey> keys = findMatches(minTime, maxTime, conditions).stream().map((e) -> e.getKey()).collect(Collectors.toList());
		Runnable deleteAllTask = new DeleteMultipleTask(recoveryManager, this, keys);
		Future<?> future = executor.submit(deleteAllTask);
		try {
			future.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			throw new BlueDbException("delete query failed", e);
		}
	}

	public void updateAll(long minTime, long maxTime, List<Condition<T>> conditions, Updater<T> updater) throws BlueDbException {
		List<BlueEntity> entities = findMatches(minTime, maxTime, conditions);
		Runnable updateTask = new UpdateMultipleTask(recoveryManager, this, entities, updater);
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
			List<BlueEntity> entitesInSegment = segment.read(minTime, maxTime);
			for (BlueEntity entity: entitesInSegment) {
				T value = (T)entity.getObject();
				if(Blutils.meetsConditions(conditions, value)) {
					results.add(entity);
				}
			}
		}
		return results;
	}

	@Override
	public boolean contains(BlueKey key) throws BlueDbException {
		return get(key) != null;
	}

	public Path getPath() {
		return path;
	}
	
	public void shutdown() {
		// TODO shutdown executors? what else?
	}

	public List<Segment> getSegments(BlueKey key) {
		List<Segment> segments = new ArrayList<>();
		if (key instanceof TimeFrameKey) {
			TimeFrameKey timeFrameKey = (TimeFrameKey)key;
			for (Long l: SegmentIdConverter.getSegments(timeFrameKey.getStartTime(), timeFrameKey.getEndTime())) {
				segments.add(new Segment(path, l));
			}
		} else if (key instanceof TimeKey) {
			TimeKey timeKey = (TimeKey)key;
			long segmentId = SegmentIdConverter.convertTimeToSegmentId(timeKey.getTime());
			segments.add(new Segment(path, segmentId));
		} else {
			segments.add(new Segment(path, key.toString())); // TODO break into safely named segments
		}
		return segments;
	}

	private List<Segment> getSegments(long minTime, long maxTime) {
		long minSegmentId = SegmentIdConverter.convertTimeToSegmentId(minTime);
		long maxSegmentId = SegmentIdConverter.convertTimeToSegmentId(maxTime);
		List<Segment> segments = new ArrayList<>();
		// TODO this should be way better
		List<File> segmentFiles = Blutils.listFiles(path, ".segment");
		for (File segmentFile: segmentFiles) {
			String fileName = segmentFile.getName();
			String segmentIdStr = fileName.substring(0, fileName.indexOf(".segment"));
			long segmentId = Long.parseLong(segmentIdStr);
			if (segmentId >= minSegmentId && segmentId <= maxSegmentId) {
				segments.add(new Segment(path, segmentId));
			}
		}
		return segments;
	}
}
