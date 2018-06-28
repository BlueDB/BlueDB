package io.bluedb.disk.collection;

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
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.BlueDbOnDisk;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.collection.task.DeleteMultipleTask;
import io.bluedb.disk.collection.task.DeleteTask;
import io.bluedb.disk.collection.task.InsertTask;
import io.bluedb.disk.collection.task.UpdateMultipleTask;
import io.bluedb.disk.collection.task.UpdateTask;
import io.bluedb.disk.file.FileManager;
import io.bluedb.disk.query.BlueQueryImpl;
import io.bluedb.disk.recovery.RecoveryManager;
import io.bluedb.disk.segment.BlueEntity;
import io.bluedb.disk.segment.Segment;
import io.bluedb.disk.segment.SegmentManager;
import io.bluedb.disk.serialization.BlueSerializer;
import io.bluedb.disk.serialization.ThreadLocalFstSerializer;

public class BlueCollectionImpl<T extends Serializable> implements BlueCollection<T> {

	ExecutorService executor = Executors.newFixedThreadPool(1);

	private final RecoveryManager<T> recoveryManager;
	private final Path path;
	private final FileManager fileManager;
	private final SegmentManager<T> segmentManager;

	public BlueCollectionImpl(BlueDbOnDisk db, String name, Class<T> type) {
		path = Paths.get(db.getPath().toString(), name);
		path.toFile().mkdirs();
		BlueSerializer serializer = new ThreadLocalFstSerializer(type);
		fileManager = new FileManager(serializer);
		segmentManager = new SegmentManager<T>(this);
		recoveryManager = new RecoveryManager<T>(this, fileManager, serializer);
		recoveryManager.recover();  // everything else has to be in place before running this
	}

	@Override
	public BlueQuery<T> query() {
		return new BlueQueryImpl<T>(this);
	}

	@Override
	public boolean contains(BlueKey key) throws BlueDbException {
		return get(key) != null;
	}

	@Override
	public T get(BlueKey key) throws BlueDbException {
		Segment<T> firstSegment = segmentManager.getFirstSegment(key);
		return firstSegment.get(key);
	}

	public List<T> getList(long minTime, long maxTime, List<Condition<T>> conditions) throws BlueDbException {
		 return (List<T>) findMatches(minTime, maxTime, conditions).stream().map((e) -> e.getObject()).collect(Collectors.toList());
	}

	@Override
	public void insert(BlueKey key, T value) throws BlueDbException {
		Runnable insertTask = new InsertTask<T>(this, key, value);
		Future<?> future = executor.submit(insertTask);
		try {
			future.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			throw new BlueDbException("insert failed for key " + key.toString(), e);
		}
	}

	@Override
	public void update(BlueKey key, Updater<T> updater) throws BlueDbException {
		Runnable updateTask = new UpdateTask<T>(this, key, updater);
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
		Runnable deleteTask = new DeleteTask<T>(this, key);
		Future<?> future = executor.submit(deleteTask);
		try {
			future.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			throw new BlueDbException("delete failed for key " + key.toString(), e);
		}
	}

	public void updateAll(long minTime, long maxTime, List<Condition<T>> conditions, Updater<T> updater) throws BlueDbException {
		List<BlueEntity<T>> entities = findMatches(minTime, maxTime, conditions);
		Runnable updateTask = new UpdateMultipleTask<T>(this, entities, updater);
		Future<?> future = executor.submit(updateTask);
		try {
			future.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			throw new BlueDbException("update query failed", e);
		}
	}

	public void deleteAll(long minTime, long maxTime, List<Condition<T>> conditions) throws BlueDbException {
		List<BlueKey> keys = findMatches(minTime, maxTime, conditions).stream().map((e) -> e.getKey()).collect(Collectors.toList());
		Runnable deleteAllTask = new DeleteMultipleTask<T>(this, keys);
		Future<?> future = executor.submit(deleteAllTask);
		try {
			future.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			throw new BlueDbException("delete query failed", e);
		}
	}

	private List<BlueEntity<T>> findMatches(long minTime, long maxTime, List<Condition<T>> conditions) throws BlueDbException {
		List<BlueEntity<T>> results = new ArrayList<>();
		List<Segment<T>> segments = segmentManager.getExistingSegments(minTime, maxTime);
		for (Segment<T> segment: segments) {
			List<BlueEntity<T>> entitesInSegment = segment.getRange(minTime, maxTime);
			for (BlueEntity<T> entity: entitesInSegment) {
				T value = entity.getObject();
				if(Blutils.meetsConditions(conditions, value)) {
					results.add(entity);
				}
			}
		}
		return results;
	}

	public SegmentManager<T> getSegmentManager() {
		return segmentManager;
	}

	public RecoveryManager<T> getRecoveryManager() {
		return recoveryManager;
	}

	public Path getPath() {
		return path;
	}

	public FileManager getFileManager() {
		return fileManager;
	}

	public void shutdown() {
		executor.shutdown();
	}
}
