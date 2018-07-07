package io.bluedb.disk.segment;

import java.io.File;
import java.io.FileFilter;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.exceptions.DuplicateKeyException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.file.BlueObjectInput;
import io.bluedb.disk.file.BlueObjectOutput;
import io.bluedb.disk.file.BlueReadLock;
import io.bluedb.disk.file.BlueWriteLock;
import io.bluedb.disk.file.FileManager;
import io.bluedb.disk.file.LockManager;
import io.bluedb.disk.serialization.BlueEntity;

public class Segment <T extends Serializable> {

	private final static Long SEGMENT_SIZE = SegmentManager.LEVEL_0;
	private final static Long[] ROLLUP_LEVELS = {1L, 3125L, SEGMENT_SIZE};

	private final FileManager fileManager;
	private final Path segmentPath;
	private final LockManager<Path> lockManager;

	public Segment(Path segmentPath, FileManager fileManager) {
		this.segmentPath = segmentPath;
		this.fileManager = fileManager;
		lockManager = fileManager.getLockManager();
	}

	// for testing only
	protected Segment() {segmentPath = null;fileManager = null;lockManager = null;}

	@Override
	public String toString() {
		return "<Segment for path " + segmentPath.toString() + ">";
	}

	public boolean contains(BlueKey key) throws BlueDbException {
		return get(key) != null;
	}

	interface Processor<X extends Serializable> {
		public void process(BlueObjectInput<BlueEntity<X>> input, BlueObjectOutput<BlueEntity<X>> output) throws BlueDbException;
	}

	public void update(BlueKey newKey, T newValue) throws BlueDbException {
		long groupingNumber = newKey.getGroupingNumber();
		modifyChunk(groupingNumber, new Processor<T>() {
			@Override
			public void process(BlueObjectInput<BlueEntity<T>> input, BlueObjectOutput<BlueEntity<T>> output) throws BlueDbException {
				BlueEntity<T> newEntity = new BlueEntity<T>(newKey, newValue);
				while (input.hasNext()) {
					BlueEntity<T> iterEntity = input.next();
					BlueKey iterKey = iterEntity.getKey();
					if (iterKey.equals(newKey)) {
						output.write(newEntity);
						newEntity = null;
					} else if (newEntity != null && iterKey.getGroupingNumber() > groupingNumber) {
						output.write(newEntity);
						newEntity = null;
						output.write(iterEntity);
					} else {
						output.write(iterEntity);
					}
				}
				if (newEntity != null) {
					output.write(newEntity);
				}
			}
		});
	}

	public void insert(BlueKey newKey, T newValue) throws BlueDbException {
		BlueEntity<T> newEntity = new BlueEntity<T>(newKey, newValue);
		long groupingNumber = newKey.getGroupingNumber();
		modifyChunk(groupingNumber, new Processor<T>() {
			@Override
			public void process(BlueObjectInput<BlueEntity<T>> input, BlueObjectOutput<BlueEntity<T>> output) throws BlueDbException {
				BlueEntity<T> toInsert = newEntity;
				while (input.hasNext()) {
					BlueEntity<T> iterEntity = input.next();
					BlueKey iterKey = iterEntity.getKey();
					if (iterKey.equals(newKey)) {
						throw new DuplicateKeyException("attempt to insert duplicate key", newKey);
					} else if (toInsert != null && iterKey.getGroupingNumber() > groupingNumber) {
						output.write(newEntity);
						toInsert = null;
						output.write(iterEntity);
					} else {
						output.write(iterEntity);
					}
				}
				if (toInsert != null) {
					output.write(newEntity);
				}
			}
		});
	}

	public void delete(BlueKey key) throws BlueDbException {
		long groupingNumber = key.getGroupingNumber();
		modifyChunk(groupingNumber, new Processor<T>() {
			@Override
			public void process(BlueObjectInput<BlueEntity<T>> input, BlueObjectOutput<BlueEntity<T>> output) throws BlueDbException {
				while (input.hasNext()) {
					BlueEntity<T> entry = input.next();
					if (!entry.getKey().equals(key)) {
						output.write(entry);
					}
				}
			}
		});
	}

	public void modifyChunk(long groupingNumber, Processor<T> processor) throws BlueDbException {
		Path targetPath, tmpPath;

		try (BlueReadLock<Path> readLock = getReadLockFor(groupingNumber)) {
			targetPath = readLock.getKey();
			FileManager.ensureFileExists(targetPath);
			tmpPath = FileManager.createTempFilePath(targetPath);
			try (BlueWriteLock<Path> tempFileLock = lockManager.acquireWriteLock(tmpPath)) {
				try(BlueObjectOutput<BlueEntity<T>> output = fileManager.getBlueOutputStream(tempFileLock)) {
					try(BlueObjectInput<BlueEntity<T>> input = fileManager.getBlueInputStream(readLock)) {
						processor.process(input, output);
					}
				}
			}
		}

		try (BlueWriteLock<Path> targetFileLock = lockManager.acquireWriteLock(targetPath)) {
			FileManager.moveFile(tmpPath, targetFileLock);
		}
	}

	public T get(BlueKey key) throws BlueDbException {
		try (BlueReadLock<Path> readLock = getReadLockFor(key)) {
			if (!readLock.getKey().toFile().exists()) {
				return null;
			}
			try(BlueObjectInput<BlueEntity<T>> inputStream = fileManager.getBlueInputStream(readLock)) {
				return get(key, inputStream);
			}
		}
	}

	public List<T> getAll() throws BlueDbException {
		return getRange(Long.MIN_VALUE, Long.MAX_VALUE)
				.stream()
				.map((e) -> e.getValue())
				.collect(Collectors.toList());
	}

	public List<BlueEntity<T>> getRange(long min, long max) throws BlueDbException {
		// We can't bound from below.  A query for [2,4] should return a TimeRangeKey [1,3] which would be stored at 1.
		long minGroupingNumber = Long.MIN_VALUE;
		List<File> relevantFiles = getOrderedFilesInRange(Long.MIN_VALUE, max);
		List<BlueEntity<T>> results = new ArrayList<>();
		for (File file: relevantFiles) {
			TimeRange rangeForThisFile = TimeRange.fromUnderscoreDelmimitedString(file.getName());
			if (minGroupingNumber > rangeForThisFile.getEnd()) {
				continue;  // we've already read the rolled up file that includes this range
			}
			try (BlueReadLock<Path> readLock = getReadLockFor(rangeForThisFile.getStart())) { // get the rolled up file if applicable
				if (!readLock.getKey().toFile().exists())
					continue;
				try(BlueObjectInput<BlueEntity<T>> inputStream = fileManager.getBlueInputStream(readLock)) {
					while(inputStream.hasNext()) {
						BlueEntity<T> next = inputStream.next();
						BlueKey key = next.getKey();
						if (key.getGroupingNumber() < minGroupingNumber) {
							continue;
						}
						if (Blutils.isInRange(key, min, max))
							results.add(next);
					}
				}
			}
			minGroupingNumber = rangeForThisFile.getEnd() + 1;
		}
		return results;
    }

	public void rollup(long start, long end) throws BlueDbException {
		long rollupSize = end - start + 1;
		boolean isValidRollupSize = Arrays.asList(ROLLUP_LEVELS).contains(rollupSize);
		if (!isValidRollupSize) {
			throw new BlueDbException("Rollup range [" + start + "," + end + "] not a valid rollup size");
		}
		List<File> filesToRollup = getOrderedFilesInRange(start, end);
		Path path = Paths.get(segmentPath.toString(), start + "_" + end);
		Path tmpPath = FileManager.createTempFilePath(path);

		copy(tmpPath, filesToRollup);
		moveRolledUpFileAndDeleteSourceFiles(path, tmpPath, filesToRollup);
	}

	private void copy(Path destination, List<File> sources) throws BlueDbException {
		try (BlueWriteLock<Path> tempFileLock = lockManager.acquireWriteLock(destination)) {
			try(BlueObjectOutput<BlueEntity<T>> outputStream = fileManager.getBlueOutputStream(tempFileLock)) {
				for (File file: sources) {
					try(BlueReadLock<Path> readLock = lockManager.acquireReadLock(file.toPath())) {
						try(BlueObjectInput<BlueEntity<T>> inputStream = fileManager.getBlueInputStream(readLock)) {
							Blutils.copyObjects(inputStream, outputStream);
						}
					}
				}
			}
		}
	}

	private void moveRolledUpFileAndDeleteSourceFiles(Path newRolledupPath, Path tempRolledupPath, List<File> filesToRollup) throws BlueDbException {
		List<BlueWriteLock<Path>> sourceFileWriteLocks = new ArrayList<>();
		try (BlueWriteLock<Path> targetFileLock = lockManager.acquireWriteLock(newRolledupPath)){
			for (File file: filesToRollup) {
				sourceFileWriteLocks.add(lockManager.acquireWriteLock(file.toPath()));
			}

			// TODO figure out how to recover if we crash here, must be done before any writes
			FileManager.moveFile(tempRolledupPath, targetFileLock);
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
	protected List<File> getOrderedFilesInRange(long min, long max) {
		File segmentFolder = segmentPath.toFile();
		FileFilter filter = (f) -> doesfileNameRangeOverlap(f, min, max);
		List<File> filesInFolder = FileManager.getFolderContents(segmentFolder, filter);
		Comparator<File> comparator = new Comparator<>() {
			@Override
			public int compare(File o1, File o2) {
				TimeRange r1 = TimeRange.fromUnderscoreDelmimitedString(o1.getName());
				TimeRange r2 = TimeRange.fromUnderscoreDelmimitedString(o2.getName());
				return r1.compareTo(r2);
			}
		};
		Collections.sort(filesInFolder, comparator);
		return filesInFolder;
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
		long groupingNumber = key.getGroupingNumber();
		return getReadLockFor(groupingNumber);
	}

	protected BlueReadLock<Path> getReadLockFor(long groupingNumber) {
		for (long rollupLevel: ROLLUP_LEVELS) {
			Path path = getPathFor(groupingNumber, rollupLevel);
			BlueReadLock<Path> lock = lockManager.acquireReadLock(path);
			try {
				if (lock.getKey().toFile().exists()) {
					return lock;
				}
			} catch (Throwable t) { // make sure we don't hold onto the lock if there's an exception
			}
			lock.release();
		}
		Path path = getPathFor(groupingNumber, 1);
		return lockManager.acquireReadLock(path);
	}

	protected Path getPath() {
		return segmentPath;
	}

	private Path getPathFor(long groupingNumber, long rollupLevel) {
		String fileName = SegmentManager.getRangeFileName(groupingNumber, rollupLevel);
		return Paths.get(segmentPath.toString(), fileName);
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
