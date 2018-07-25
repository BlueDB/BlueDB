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

import io.bluedb.api.BlueCollection;
import io.bluedb.api.BlueQuery;
import io.bluedb.api.Condition;
import io.bluedb.api.Updater;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.BlueDbOnDisk;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.collection.task.DeleteTask;
import io.bluedb.disk.collection.task.InsertTask;
import io.bluedb.disk.collection.task.UpdateTask;
import io.bluedb.disk.file.FileManager;
import io.bluedb.disk.query.BlueQueryOnDisk;
import io.bluedb.disk.recovery.RecoveryManager;
import io.bluedb.disk.segment.Segment;
import io.bluedb.disk.segment.SegmentManager;
import io.bluedb.disk.segment.rollup.RollupScheduler;
import io.bluedb.disk.segment.rollup.RollupTask;
import io.bluedb.disk.segment.Range;
import io.bluedb.disk.serialization.BlueEntity;
import io.bluedb.disk.serialization.BlueSerializer;
import io.bluedb.disk.serialization.ThreadLocalFstSerializer;

public class BlueCollectionOnDisk<T extends Serializable> implements BlueCollection<T> {

	ExecutorService executor = Executors.newFixedThreadPool(1);

	private final Class<T> valueType;
	private final Class<? extends BlueKey> keyType;
	private final BlueSerializer serializer;
	private final RecoveryManager<T> recoveryManager;
	private final Path collectionPath;
	private final FileManager fileManager;
	private final SegmentManager<T> segmentManager;
	private final RollupScheduler rollupScheduler;
	private final CollectionMetaData metaData;

	public BlueCollectionOnDisk(BlueDbOnDisk db, String name, Class<T> valueType, Class<? extends BlueKey> requestedKeyType) throws BlueDbException {
		this.valueType = valueType;
		collectionPath = Paths.get(db.getPath().toString(), name);
		collectionPath.toFile().mkdirs();
		metaData = new CollectionMetaData(collectionPath);
		Class<? extends Serializable>[] classesToRegister = metaData.getAndAddToSerializedClassList(valueType);
		serializer = new ThreadLocalFstSerializer(classesToRegister);
		fileManager = new FileManager(serializer);
		this.keyType = determineKeyType(metaData, requestedKeyType);
		segmentManager = new SegmentManager<T>(this, this.keyType);
		recoveryManager = new RecoveryManager<T>(this, fileManager, serializer);
		rollupScheduler = new RollupScheduler(this);
		rollupScheduler.start();
		recoveryManager.recover();  // everything else has to be in place before running this
	}

	@Override
	public BlueQuery<T> query() {
		return new BlueQueryOnDisk<T>(this);
	}

	@Override
	public boolean contains(BlueKey key) throws BlueDbException {
		ensureCorrectKeyType(key);
		return get(key) != null;
	}

	@Override
	public T get(BlueKey key) throws BlueDbException {
		ensureCorrectKeyType(key);
		Segment<T> firstSegment = segmentManager.getFirstSegment(key);
		return firstSegment.get(key);
	}

	@Override
	public void insert(BlueKey key, T value) throws BlueDbException {
		ensureCorrectKeyType(key);
		// TODO roll up to a smaller time range?
		// TODO report insert when the insert task actually runs?
		Range timeRange = segmentManager.getSegmentRange(key.getGroupingNumber());
		rollupScheduler.reportInsert(timeRange);
		Runnable insertTask = new InsertTask<T>(this, key, value);
		executeTask(insertTask);
	}

	@Override
	public void update(BlueKey key, Updater<T> updater) throws BlueDbException {
		ensureCorrectKeyType(key);
		Runnable updateTask = new UpdateTask<T>(this, key, updater);
		executeTask(updateTask);
	}

	@Override
	public void delete(BlueKey key) throws BlueDbException {
		ensureCorrectKeyType(key);
		Runnable deleteTask = new DeleteTask<T>(this, key);
		executeTask(deleteTask);
	}

	public List<BlueEntity<T>> findMatches(long min, long max, List<Condition<T>> conditions) throws BlueDbException {
		List<BlueEntity<T>> results = new ArrayList<>();
		try (CollectionEntityIterator<T> iterator = new CollectionEntityIterator<T>(this, min, max)) {
			while (iterator.hasNext()) {
				BlueEntity<T> entity = iterator.next();
				T value = entity.getValue();
				if(Blutils.meetsConditions(conditions, value)) {
					results.add(entity);
				}
			}
		}
		return results;
	}

	public void executeTask(Runnable task) throws BlueDbException{
		Future<?> future = executor.submit(task);
		try {
			future.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			throw new BlueDbException("BlueDB task failed " + task.toString(), e);
		}
	}

	public void rollup(Range timeRange) throws BlueDbException {
		Segment<T> segment = segmentManager.getSegment(timeRange.getStart());
		segment.rollup(timeRange);
	}

	public void scheduleRollup(Range timeRange) {
		Runnable rollupRunnable = new RollupTask<T>(this, timeRange);
		executor.submit(rollupRunnable);
	}

	public SegmentManager<T> getSegmentManager() {
		return segmentManager;
	}

	public RecoveryManager<T> getRecoveryManager() {
		return recoveryManager;
	}

	public Path getPath() {
		return collectionPath;
	}

	public FileManager getFileManager() {
		return fileManager;
	}

	public BlueSerializer getSerializer() {
		return serializer;
	}

	public CollectionMetaData getMetaData() {
		return metaData;
	}

	public void shutdown() {
		rollupScheduler.stop();
		executor.shutdownNow();
	}

	public Class<T> getType() {
		return valueType;
	}

	public Class<? extends BlueKey> getKeyType() {
		return keyType;
	}

	@Override
	public Long getMaxLongId() throws BlueDbException {
		return metaData.getMaxLong();
	}

	@Override
	public Integer getMaxIntegerId() throws BlueDbException {
		return metaData.getMaxInteger();
	}

	protected void ensureCorrectKeyType(BlueKey key) throws BlueDbException {
		if (!keyType.isAssignableFrom(key.getClass())) {
			throw new BlueDbException("wrong key type (" + key.getClass() + ") for Collection with key type " + keyType);
		}
	}

	protected static Class<? extends BlueKey> determineKeyType(CollectionMetaData metaData, Class<? extends BlueKey> providedKeyType) throws BlueDbException {
		Class<? extends BlueKey> storedKeyType = metaData.getKeyType();
		if (storedKeyType == null) {
			metaData.saveKeyType(providedKeyType);
			return providedKeyType;
		} else if (providedKeyType == null) {
			return storedKeyType;
		} else if (!providedKeyType.isAssignableFrom(storedKeyType)){
			throw new BlueDbException("Cannot instantiate a Collection<" + providedKeyType + "> from a Collection<" + storedKeyType + ">");
		} else {
			return providedKeyType;
		}
	}
}
