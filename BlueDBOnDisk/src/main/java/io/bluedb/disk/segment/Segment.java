package io.bluedb.disk.segment;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.file.BlueObjectInput;
import io.bluedb.disk.file.BlueReadLock;
import io.bluedb.disk.file.BlueWriteLock;
import io.bluedb.disk.file.FileManager;
import io.bluedb.disk.file.LockManager;
import io.bluedb.disk.serialization.BlueEntity;

public class Segment <T extends Serializable> {

	private final static long SEGMENT_SIZE = SegmentManager.LEVEL_0;
	private final static long[] ROLLUP_LEVELS = {1, 3125, SEGMENT_SIZE};

	private final FileManager fileManager;
	private final Path segmentPath;

	public Segment(Path segmentPath, FileManager fileManager) {
		this.segmentPath = segmentPath;
		this.fileManager = fileManager;
	}

	@Override
	public String toString() {
		return "<Segment for path " + segmentPath.toString() + ">";
	}

	public boolean contains(BlueKey key) throws BlueDbException {
		return get(key) != null;
	}

	public void save(BlueKey key, T value) throws BlueDbException {
		ArrayList<BlueEntity<T>> entities = null;
		File file;
		try (BlueReadLock<Path> readLock = getReadLockFor(key)) {
			if (readLock != null) {
				entities = fetch(readLock);
				file = readLock.getKey().toFile();
			} else {
				entities = new ArrayList<>();
				file = 	getPathFor(key, 1).toFile();
			}
		}
		BlueEntity<T> newEntity = new BlueEntity<T>(key, value);
		remove(key, entities);
		entities.add(newEntity);
		persist(file.toPath(), entities);
	}

	public void delete(BlueKey key) throws BlueDbException {
		ArrayList<BlueEntity<T>> entities = null;
		File file;
		try (BlueReadLock<Path> readLock = getReadLockFor(key)) {
			if (readLock == null) {
				return;
			}
			entities = fetch(readLock);
			file = readLock.getKey().toFile();
		}
		remove(key, entities);
		persist(file.toPath(), entities);
	}

	public T get(BlueKey key) throws BlueDbException {
		try (BlueReadLock<Path> readLock = getReadLockFor(key)) {
			if (readLock == null ) {
				return null;
			}
			try(BlueObjectInput<BlueEntity<T>> inputStream = fileManager.getBlueInputStream(readLock)) {
				return get(key, inputStream);
			} catch (IOException e) {
				e.printStackTrace();
				Path path = readLock.getKey();
				throw new BlueDbException("error reading file " + path , e);
			}
		}
	}

	public List<T> getAll() throws BlueDbException {
		List<File> filesInFolder = FileManager.getFolderContents(segmentPath.toFile());
		List<T> results = new ArrayList<>();
		for (File file: filesInFolder) {
			for (BlueEntity<T> entity: fetch(file)) {
				results.add(entity.getValue());
			}
		}
		return results;
	}

    public List<BlueEntity<T>> getRange(long minTime, long maxTime) throws BlueDbException {
		List<BlueEntity<T>> results = new ArrayList<>();
		List<File> filesInFolder = FileManager.getFolderContents(segmentPath.toFile());
		// Note that we cannot bound this from below because a TimeRangeKey that overlaps the target range
		//      will be stored at the start time;
		List<File> relevantFiles = Blutils.filter(filesInFolder, (f) -> doesfileNameRangeOverlap(f, Long.MIN_VALUE, maxTime));
		for (File file: relevantFiles) {
			List<BlueEntity<T>> fileContents = fetch(file);
			for (BlueEntity<T> entity: fileContents) {
				BlueKey key = entity.getKey();
				if (inTimeRange(minTime, maxTime, key)) {
					results.add(entity);
				}
			}
		}
		return results;
	}

	// TODO make private?  or somehow enforce standard levels
	public void rollup(long start, long end) throws BlueDbException {
		List<File> filesToRollup = getFilesInRange(start, end);
		Path path = Paths.get(segmentPath.toString(), start + "_" + end);
		Path tmpPath = FileManager.createTempFilePath(path);
		List<BlueEntity<T>> entities = new ArrayList<>();

		// TODO switch to streaming
		// TODO keep sorted, here and everywhere you write
		for (File file: filesToRollup) {
			List<BlueEntity<T>> es = fetch(file);
			entities.addAll(es);
		}
		LockManager<Path> lockManager = fileManager.getLockManager();
		try (BlueWriteLock<Path> tempFileLock = lockManager.acquireWriteLock(tmpPath)) {
			fileManager.saveList(tempFileLock, entities);
		}

		List<BlueWriteLock<Path>> sourceFileWriteLocks = new ArrayList<>();
		try (BlueWriteLock<Path> targetFileLock = lockManager.acquireWriteLock(path)){
			for (File file: filesToRollup) {
				sourceFileWriteLocks.add(lockManager.acquireWriteLock(file.toPath()));
			}

			FileManager.moveFile(tmpPath, targetFileLock);
			for (BlueWriteLock<Path> writeLock: sourceFileWriteLocks) {
				FileManager.deleteFile(writeLock);
			}
		} finally {
			for (BlueWriteLock<Path> lock: sourceFileWriteLocks) {
				lock.release();
			}
		}
	}

	// TODO test
	protected List<File> getFilesInRange(long min, long max) {
		List<File> filesInFolder = FileManager.getFolderContents(segmentPath.toFile());
		return Blutils.filter(filesInFolder, (f) -> doesfileNameRangeOverlap(f, min, max));
	}

	protected static boolean doesfileNameRangeOverlap(File file, long min, long max ) {
		try {
			String[] splits = file.getName().split("_");
			if (splits.length < 2) {
				return false;
			}
			long start = Long.valueOf(splits[0]);
			long end = Long.valueOf(splits[1]);
			return (start <= max) && (end >= min);
		} catch (Throwable t) {
			return false;
		}
	}

	protected BlueReadLock<Path> getReadLockFor(BlueKey key) {
		LockManager<Path> lockManager = fileManager.getLockManager();
		for (long rollupLevel: ROLLUP_LEVELS) {
			Path path = getPathFor(key, rollupLevel);
			BlueReadLock<Path> lock = lockManager.acquireReadLock(path);
			try {
				if (lock.getKey().toFile().exists()) {
					return lock;
				}
			} catch (Throwable t) { // make sure we don't hold onto the lock if there's an exception
			}
			lock.release();
		}
		return null;
	}

	protected Path getPath() {
		return segmentPath;
	}

	private Path getPathFor(BlueKey key, long rollupLevel) {
		long groupingNumber = key.getGroupingNumber();
		String fileName = SegmentManager.getRangeFileName(groupingNumber, rollupLevel);
		return Paths.get(segmentPath.toString(), fileName);
	}

	private ArrayList<BlueEntity<T>> fetch(File file) throws BlueDbException {
		Path path = file.toPath();
		LockManager<Path> lockManager = fileManager.getLockManager();
		try (BlueReadLock<Path> pathReadLock = lockManager.acquireReadLock(path)) {
			return fetch(pathReadLock);
		}
	}

	private ArrayList<BlueEntity<T>> fetch(BlueReadLock<Path> pathLock) throws BlueDbException {
		if (pathLock == null || !pathLock.getKey().toFile().exists()) // TODO better handling of possible exceptions
			return new ArrayList<BlueEntity<T>>();
		ArrayList<BlueEntity<T>> fileContents =  fileManager.loadList(pathLock);
		return fileContents;
	}

	private void persist(Path path, ArrayList<BlueEntity<T>> entites) throws BlueDbException {
		Path tmpPath = FileManager.createTempFilePath(path);
		LockManager<Path> lockManager = fileManager.getLockManager();
		try (BlueWriteLock<Path> tempFileLock = lockManager.acquireWriteLock(tmpPath)) {
			fileManager.saveList(tempFileLock, entites);
		}
		try (BlueWriteLock<Path> targetFileLock = lockManager.acquireWriteLock(path)) {
			FileManager.moveFile(tmpPath, targetFileLock);
		}

	}

	private static boolean inTimeRange(long minTime, long maxTime, BlueKey key) {
		if (key instanceof TimeFrameKey) {
			TimeFrameKey timeFrameKey = (TimeFrameKey) key;
			return timeFrameKey.getEndTime() >= minTime && timeFrameKey.getStartTime() <= maxTime;
		} else {
			return key.getGroupingNumber() >= minTime && key.getGroupingNumber() <= maxTime;
		}
	}

	protected static <T extends Serializable> T remove(BlueKey key, ArrayList<BlueEntity<T>> entities) {
		for (int i = 0; i < entities.size(); i++) {
			BlueEntity<T> entity = entities.get(i);
			if (entity.getKey().equals(key)) {
				entities.remove(i);
				return entity.getValue();
			}
		}
		return null;
	}

	protected static <T extends Serializable> boolean contains(BlueKey key, List<BlueEntity<T>> entities) {
		return get(key, entities) != null;
	}

	protected static <T extends Serializable> T get(BlueKey key, BlueObjectInput<BlueEntity<T>> inputStream) {
		while(inputStream.hasNext()) {
			BlueEntity<T> next = inputStream.next();
			if (next.getKey().equals(key)) {
				return next.getValue();
			}
		}
		return null;
	}
	protected static <T extends Serializable> T get(BlueKey key, List<BlueEntity<T>> entities) {
		for (BlueEntity<T> entity: entities) {
			if (entity.getKey().equals(key)) {
				return entity.getValue();
			}
		}
		return null;
	}

	@Override
	public int hashCode() {
		return 31 + ((segmentPath == null) ? 0 : segmentPath.hashCode());
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Segment)) {
			return false;
		}
		Segment<?> other = (Segment<?>) obj;
		if (segmentPath == null) {
			return other.segmentPath == null;
		} else {
			return segmentPath.equals(other.segmentPath);
		}
	}
}
