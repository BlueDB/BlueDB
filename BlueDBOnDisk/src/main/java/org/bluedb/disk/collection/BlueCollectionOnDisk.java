package org.bluedb.disk.collection;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.BlueQuery;
import org.bluedb.api.Condition;
import org.bluedb.api.Mapper;
import org.bluedb.api.Updater;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.BlueDbOnDisk;
import org.bluedb.disk.collection.index.BlueIndexOnDisk;
import org.bluedb.disk.collection.index.IndexManager;
import org.bluedb.disk.collection.task.BatchChangeTask;
import org.bluedb.disk.collection.task.BatchDeleteTask;
import org.bluedb.disk.collection.task.DeleteTask;
import org.bluedb.disk.collection.task.InsertTask;
import org.bluedb.disk.collection.task.ReplaceTask;
import org.bluedb.disk.collection.task.UpdateTask;
import org.bluedb.disk.file.FileManager;
import org.bluedb.disk.query.BlueQueryOnDisk;
import org.bluedb.disk.recovery.RecoveryManager;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.Segment;
import org.bluedb.disk.segment.SegmentManager;
import org.bluedb.disk.segment.rollup.RollupScheduler;
import org.bluedb.disk.segment.rollup.RollupTarget;
import org.bluedb.disk.segment.rollup.Rollupable;
import org.bluedb.disk.serialization.BlueEntity;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;

import io.bluedb.util.CachedSingleThreadingPool;

public class BlueCollectionOnDisk<T extends Serializable> implements BlueCollection<T>, Rollupable {

	final static CachedSingleThreadingPool executor = new CachedSingleThreadingPool();

	private final Class<T> valueType;
	private final Class<? extends BlueKey> keyType;
	private final BlueSerializer serializer;
	private final RecoveryManager<T> recoveryManager;
	private final Path collectionPath;
	private final FileManager fileManager;
	private final SegmentManager<T> segmentManager;
	private final RollupScheduler rollupScheduler;
	private final CollectionMetaData metaData;
	private final IndexManager<T> indexManager;
	private final ScheduledThreadPoolExecutor sharedExecutor;

	public BlueCollectionOnDisk(BlueDbOnDisk db, String name, Class<? extends BlueKey> requestedKeyType, Class<T> valueType, List<Class<? extends Serializable>> additionalRegisteredClasses) throws BlueDbException {
		sharedExecutor = db.getSharedExecutor();
		this.valueType = valueType;
		collectionPath = Paths.get(db.getPath().toString(), name);
		collectionPath.toFile().mkdirs();
		metaData = new CollectionMetaData(collectionPath);
		Class<? extends Serializable>[] classesToRegister = metaData.getAndAddToSerializedClassList(valueType, additionalRegisteredClasses);
		serializer = new ThreadLocalFstSerializer(classesToRegister);
		fileManager = new FileManager(serializer);
		this.keyType = determineKeyType(metaData, requestedKeyType);
		recoveryManager = new RecoveryManager<T>(this, fileManager, serializer);
		rollupScheduler = new RollupScheduler(this);
		segmentManager = new SegmentManager<T>(collectionPath, fileManager, this, this.keyType);
		indexManager = new IndexManager<>(this, collectionPath);
		rollupScheduler.start();
		recoveryManager.recover();  // everything else has to be in place before running this
	}

	public int getQueuedTaskCount() {
		return executor.getQueue().size();
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
		Runnable insertTask = new InsertTask<T>(this, key, value);
		executeTask(insertTask);
	}

	@Override
	public void batchUpsert(Map<BlueKey, T> values) throws BlueDbException {
		ensureCorrectKeyTypes(values.keySet());
		Runnable insertTask = new BatchChangeTask<T>(this, values);
		executeTask(insertTask);
	}

	@Override
	public void batchDelete(Collection<BlueKey> keys) throws BlueDbException {
		ensureCorrectKeyTypes(keys);
		Runnable deleteTask = new BatchDeleteTask<T>(this, keys);
		executeTask(deleteTask);
	}

	@Override
	public void replace(BlueKey key, Mapper<T> mapper) throws BlueDbException {
		ensureCorrectKeyType(key);
		Runnable updateTask = new ReplaceTask<T>(this, key, mapper);
		executeTask(updateTask);
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

	@Override
	public BlueKey getLastKey() {
		LastEntityFinder lastFinder = new LastEntityFinder(this);
		BlueEntity<?> lastEntity = lastFinder.getLastEntity();
		return lastEntity == null ? null : lastEntity.getKey();
	}

	public List<BlueEntity<T>> findMatches(Range range, List<Condition<T>> conditions, boolean byStartTime) throws BlueDbException {
		List<BlueEntity<T>> results = new ArrayList<>();
		try (CollectionEntityIterator<T> iterator = new CollectionEntityIterator<T>(segmentManager, range, byStartTime, conditions)) {
			while (iterator.hasNext()) {
				BlueEntity<T> entity = iterator.next();
				results.add(entity);
			}
		}
		return results;
	}

	public ScheduledThreadPoolExecutor getSharedExecutor() {
		return sharedExecutor;
	}

	public void submitTask(Runnable task) {
		executor.submit(collectionPath.toString(), task);
	}

	public void executeTask(Runnable task) throws BlueDbException{
		Future<?> future = executor.submit(collectionPath.toString(), task);
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
		executor.shutdown();
	}

	public Class<T> getType() {
		return valueType;
	}

	public Class<? extends BlueKey> getKeyType() {
		return keyType;
	}

	protected void ensureCorrectKeyType(BlueKey key) throws BlueDbException {
		if (!keyType.isAssignableFrom(key.getClass())) {
			throw new BlueDbException("wrong key type (" + key.getClass() + ") for Collection with key type " + keyType);
		}
	}

	protected void ensureCorrectKeyTypes(Collection<BlueKey> keys) throws BlueDbException {
		for (BlueKey key: keys) {
			ensureCorrectKeyType(key);
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

	@Override
	public <I extends ValueKey> BlueIndex<I, T> createIndex(String name, Class<I> keyType, KeyExtractor<I, T> keyExtractor) throws BlueDbException {
		return indexManager.getOrCreate(name, keyType, keyExtractor);
	}

	@Override
	public <I extends ValueKey> BlueIndex<I, T> getIndex(String indexName, Class<I> keyType) throws BlueDbException {
		return indexManager.getIndex(indexName, keyType);
	}

	public void rollupIndex(String indexName, Range range) throws BlueDbException {
		BlueIndexOnDisk<?, T> index = indexManager.getUntypedIndex(indexName);
		index.rollup(range);
	}

	public IndexManager<T> getIndexManager() {
		return indexManager;
	}

	@Override
	public void reportReads(List<RollupTarget> rollupTargets) {
		rollupScheduler.reportReads(rollupTargets);
	}

	@Override
	public void reportWrites(List<RollupTarget> rollupTargets) {
		rollupScheduler.reportWrites(rollupTargets);
	}

	public RollupScheduler getRollupScheduler() {
		return rollupScheduler;
	}
}
