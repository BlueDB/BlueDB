package io.bluedb.disk.segment;

import java.io.File;
import java.io.FileFilter;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.Blutils.CheckedFunction;
import io.bluedb.disk.file.BlueObjectInput;
import io.bluedb.disk.file.BlueObjectOutput;
import io.bluedb.disk.file.FileManager;
import io.bluedb.disk.lock.BlueReadLock;
import io.bluedb.disk.lock.BlueWriteLock;
import io.bluedb.disk.recovery.IndividualChange;
import io.bluedb.disk.segment.rollup.RollupTarget;
import io.bluedb.disk.segment.rollup.Rollupable;
import io.bluedb.disk.segment.writer.BatchWriter;
import io.bluedb.disk.segment.writer.DeleteWriter;
import io.bluedb.disk.segment.writer.InsertWriter;
import io.bluedb.disk.segment.writer.StreamingWriter;
import io.bluedb.disk.segment.writer.UpdateWriter;
import io.bluedb.disk.serialization.BlueEntity;

public class Segment <T extends Serializable> implements Comparable<Segment<T>> {

	private final Rollupable rollupable;
	private final FileManager fileManager;
	private final Path segmentPath;
	private final Range segmentRange;
	private final List<Long> rollupLevels;

	public Segment(Path segmentPath, Range segmentRange, Rollupable rollupable, FileManager fileManager, final List<Long> rollupLevels) {
		this.segmentPath = segmentPath;
		this.segmentRange = segmentRange;
		this.fileManager = fileManager;
		this.rollupLevels = rollupLevels;
		this.rollupable = rollupable;
	}

	protected static <T extends Serializable> Segment<T> getTestSegment () {
		return new Segment<T>();
	}

	protected Segment() {segmentPath = null;segmentRange = null;fileManager = null;rollupLevels = null;rollupable = null;}

	@Override
	public String toString() {
		return "<Segment for path " + segmentPath.toString() + ">";
	}

	public boolean contains(BlueKey key) throws BlueDbException {
		return get(key) != null;
	}

	public void update(BlueKey newKey, T newValue) throws BlueDbException {
		long groupingNumber = newKey.getGroupingNumber();
		modifyChunk(groupingNumber, new UpdateWriter<T>(newKey, newValue));
	}

	public void insert(BlueKey newKey, T newValue) throws BlueDbException {
		long groupingNumber = newKey.getGroupingNumber();
		modifyChunk(groupingNumber, new InsertWriter<T>(newKey, newValue));
	}

	public void delete(BlueKey key) throws BlueDbException {
		long groupingNumber = key.getGroupingNumber();
		modifyChunk(groupingNumber, new DeleteWriter<T>(key));
	}

	public void modifyChunk(long groupingNumber, StreamingWriter<T> processor) throws BlueDbException {
		Path targetPath, tmpPath;
		try (BlueObjectInput<BlueEntity<T>> input = getObjectInputFor(groupingNumber)) {
			targetPath = input.getPath();
			tmpPath = FileManager.createTempFilePath(targetPath);
			try(BlueObjectOutput<BlueEntity<T>> output = getObjectOutputFor(tmpPath)) {
				processor.process(input, output);
			}
		}

		try (BlueWriteLock<Path> targetFileLock = acquireWriteLock(targetPath)) {
			FileManager.moveFile(tmpPath, targetFileLock);
		}
		reportWrite(targetPath);
	}

	public T get(BlueKey key) throws BlueDbException {
		long groupingNumber = key.getGroupingNumber();
		try(BlueObjectInput<BlueEntity<T>> inputStream = getObjectInputFor(groupingNumber)) {
			return get(key, inputStream);
		}
	}

	public Range getRange() {
		return segmentRange;
	}

	public SegmentEntityIterator<T> getIterator(long highestGroupingNumberCompleted, Range range) {
		return new SegmentEntityIterator<>(this, highestGroupingNumberCompleted, range.getStart(), range.getEnd());
	}

	public SegmentEntityIterator<T> getIterator(long highestGroupingNumberCompleted, long rangeMin, long rangeMax) {
		return new SegmentEntityIterator<>(this, highestGroupingNumberCompleted, rangeMin, rangeMax);
	}

	public SegmentEntityIterator<T> getIterator(long min, long max) {
		return new SegmentEntityIterator<>(this, min, max);
	}

	public void applyChanges(LinkedList<IndividualChange<T>> changeQueueForSegment) throws BlueDbException {
		performPreBatchRollups();
		SegmentBatch<T> segmentBatch = new SegmentBatch<>(changeQueueForSegment);
		List<Range> existingChunkRanges = getAllFileRangesInOrder(getPath());
		List<ChunkBatch<T>> chunkBatches = segmentBatch.breakIntoChunks(existingChunkRanges, rollupLevels);
		for (ChunkBatch<T> chunkBatch: chunkBatches) {
			String fileName = chunkBatch.getRange().toUnderscoreDelimitedString();
			Path path = Paths.get(segmentPath.toString(), fileName);
			modifyChunk(path, new BatchWriter<T>(chunkBatch.getChangesInOrder()));
		}
	}

	private void performPreBatchRollups() throws BlueDbException {
		List<Range> existingChunkRanges = getAllFileRangesInOrder(getPath());
		List<Range> rangesToRollup = determineRangesRequiringRollup(existingChunkRanges, getMinimumRollupSizeBeforeBatch());
		for (Range rangeToRollup: rangesToRollup) {
			rollup(rangeToRollup, false);
		}
	}

	public long getMinimumRollupSizeBeforeBatch() {
		return rollupLevels.get(1);
	}

	public static List<Range> determineRangesRequiringRollup(List<Range> existingChunkRanges, long minimumChunkSize) {
		return existingChunkRanges.stream()
			.filter( (range) -> range.length() < minimumChunkSize )
			.map( (range) -> Range.forValueAndRangeSize(range.getStart(), minimumChunkSize) )
			.distinct()
			.collect(Collectors.toList());
	}

	public void modifyChunk(Path targetPath, StreamingWriter<T> processor) throws BlueDbException {
		Path tmpPath = FileManager.createTempFilePath(targetPath);
		BlueReadLock<Path> lock = acquireReadLock(targetPath);
		try (BlueObjectInput<BlueEntity<T>> input = fileManager.getBlueInputStream(lock)) {
			try(BlueObjectOutput<BlueEntity<T>> output = getObjectOutputFor(tmpPath)) {
				processor.process(input, output);
			}
		}
		try (BlueWriteLock<Path> targetFileLock = acquireWriteLock(targetPath)) {
			FileManager.moveFile(tmpPath, targetFileLock);
		}
		reportWrite(targetPath);
	}

	public void rollup(Range timeRange) throws BlueDbException {
		rollup(timeRange, true);
	}

	private void rollup(Range timeRange, boolean abortIfOnlyOneFile) throws BlueDbException {
		if (!isValidRollupRange(timeRange)) {
			throw new BlueDbException("Not a valid rollup size: " + timeRange);
		}
		List<File> filesToRollup = getOrderedFilesEnclosedInRange(timeRange);
		if (abortIfOnlyOneFile && filesToRollup.size() < 2) {
			return;  // no benefit to rolling up a single file
		}
		Path path = Paths.get(segmentPath.toString(), timeRange.toUnderscoreDelimitedString());
		Path tmpPath = FileManager.createTempFilePath(path);

		if (path.toFile().exists()) { // we're recovering after a rollup failed while deleting the removed files
			filesToRollup = Blutils.filter(filesToRollup, (f) -> !f.equals(path.toFile()));  // don't delete the rolled up file
			cleanupFiles(filesToRollup);
		} else {
			copy(tmpPath, filesToRollup);
			moveRolledUpFileAndDeleteSourceFiles(path, tmpPath, filesToRollup);
		}
	}

	public boolean isValidRollupRange(Range timeRange) {
		long rollupSize = timeRange.getEnd() - timeRange.getStart() + 1;  // Note: can overflow
		boolean isValidSize = rollupLevels.contains(rollupSize);
		boolean isValidStartPoint = timeRange.getStart() % rollupSize == 0;
		return isValidSize && isValidStartPoint;
	}

	void copy(Path destination, List<File> sources) throws BlueDbException {
		try(BlueObjectOutput<BlueEntity<T>> output = getObjectOutputFor(destination)) {
			for (File file: sources) {
				try(BlueObjectInput<BlueEntity<T>> inputStream = getObjectInputFor(file.toPath())) {
					output.writeAll(inputStream);
				}
			}
		}
	}

	private void cleanupFiles(List<File> filesToRollup) throws BlueDbException {
		for (File file: filesToRollup) {
			try (BlueWriteLock<Path> writeLock = acquireWriteLock(file.toPath())){
				FileManager.deleteFile(writeLock);
			}
		}
	}

	private void moveRolledUpFileAndDeleteSourceFiles(Path newRolledupPath, Path tempRolledupPath, List<File> filesToRollup) throws BlueDbException {
		List<BlueWriteLock<Path>> sourceFileWriteLocks = new ArrayList<>();
		try (BlueWriteLock<Path> targetFileLock = acquireWriteLock(newRolledupPath)){
			for (File file: filesToRollup) {
				sourceFileWriteLocks.add(acquireWriteLock(file.toPath()));
			}

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

	public static List<Range> getAllFileRangesInOrder(Path segmentPath) {
		File segmentFolder = segmentPath.toFile();
		List<File> allFilesInSegment = FileManager.getFolderContents(segmentFolder);
		List<Range> allFileRanges = allFilesInSegment.stream()
				.map( (File fl) -> Range.fromUnderscoreDelmimitedString(fl.getName()) )
				.filter((r) -> r != null)
				.collect(Collectors.toList());
		Collections.sort(allFileRanges);
		return allFileRanges;
	}

	public List<File> getOrderedFilesEnclosedInRange(Range range) {
		return getOrderedFilesEnclosedInRange(segmentPath, range);
	}

	protected static List<File> getOrderedFilesEnclosedInRange(Path segmentPath, Range range) {
		long min = range.getStart();
		long max = range.getEnd();
		File segmentFolder = segmentPath.toFile();
		FileFilter filter = (f) -> isFileNameRangeEnclosed(f, min, max);
		List<File> filesInFolder = FileManager.getFolderContents(segmentFolder, filter);
		sortByRange(filesInFolder);
		return filesInFolder;
	}

	public List<File> getOrderedFilesInRange(Range range) {
		return getOrderedFilesInRange(segmentPath, range);
	}

	protected static List<File> getOrderedFilesInRange(Path segmentPath, Range range) {
		long min = range.getStart();
		long max = range.getEnd();
		File segmentFolder = segmentPath.toFile();
		FileFilter filter = (f) -> doesfileNameRangeOverlap(f, min, max);
		List<File> filesInFolder = FileManager.getFolderContents(segmentFolder, filter);
		sortByRange(filesInFolder);
		return filesInFolder;
	}

	protected static void sortByRange(List<File> files) {
		Comparator<File> comparator = new Comparator<File>() {
			@Override
			public int compare(File o1, File o2) {
				Range r1 = Range.fromUnderscoreDelmimitedString(o1.getName());
				Range r2 = Range.fromUnderscoreDelmimitedString(o2.getName());
				return r1.compareTo(r2);
			}
		};
		Collections.sort(files, comparator);
	}

	protected static boolean isFileNameRangeEnclosed(File file, long min, long max ) {
		try {
			String[] splits = file.getName().split("_");
			if (splits.length < 2) {
				return false;
			}
			long start = Long.valueOf(splits[0]);
			long end = Long.valueOf(splits[1]);
			return (end <= max) && (start >= min);
		} catch (Throwable t) {
			return false;
		}
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

	protected BlueObjectOutput<BlueEntity<T>> getObjectOutputFor(Path path) throws BlueDbException {
		BlueWriteLock<Path> lock = acquireWriteLock(path);
		return fileManager.getBlueOutputStream(lock);
	}

	protected BlueObjectInput<BlueEntity<T>> getObjectInputFor(Path path) throws BlueDbException {
		BlueReadLock<Path> lock = acquireReadLock(path);
		return fileManager.getBlueInputStream(lock);
	}

	protected BlueObjectInput<BlueEntity<T>> getObjectInputFor(long groupingNumber) throws BlueDbException {
		BlueReadLock<Path> lock = getReadLockFor(groupingNumber);
		reportRead(lock.getKey());
		return fileManager.getBlueInputStream(lock);
	}

	public BlueReadLock<Path> getReadLockFor(long groupingNumber) throws BlueDbException {
		for (long rollupLevel: rollupLevels) {
			Path path = getPathFor(groupingNumber, rollupLevel);
			BlueReadLock<Path> lock = fileManager.getReadLockIfFileExists(path);
			if (lock != null) {
				return lock;
			}
		}
		Path path = getPathFor(groupingNumber, 1);
		return acquireReadLock(path);
	}

	public Path getPath() {
		return segmentPath;
	}

	protected void reportWrite(Path path) {
		String fileName = path.toFile().getName();
		Range targetRange = Range.fromUnderscoreDelmimitedString(fileName);
		List<RollupTarget> rollupTargets = getRollupTargets(targetRange);
		rollupable.reportWrites(rollupTargets);
	}

	protected void reportRead(Path path) {
		String fileName = path.toFile().getName();
		Range targetRange = Range.fromUnderscoreDelmimitedString(fileName);
		List<RollupTarget> rollupTargets = getRollupTargets(targetRange);
		rollupable.reportReads(rollupTargets);
	}

	protected List<RollupTarget> getRollupTargets(Range currentChunkRange) {
		List<Range> rollupRangesEnclosingChunk = getRollupRanges(currentChunkRange);
		CheckedFunction<Range, RollupTarget> rangeToTarget = (r) ->  new RollupTarget(segmentRange.getStart(), r);
		List<RollupTarget> targets = Blutils.mapIgnoringExceptions(rollupRangesEnclosingChunk, rangeToTarget);
		return targets;
	}

	protected List<Range> getRollupRanges(Range currentChunkRange) {
		long currentChunkSize = currentChunkRange.length();
		List<Long> rollupLevelsLargerThanChunk = Blutils.filter(rollupLevels, (l) -> l > currentChunkSize);
		CheckedFunction<Long, Range> toRange = (l) -> Range.forValueAndRangeSize(currentChunkRange.getStart(), l);
		List<Range> possibleRollupRanges = Blutils.mapIgnoringExceptions(rollupLevelsLargerThanChunk, toRange);
		List<Range> rollupRangesEnclosingChunk = Blutils.filter(possibleRollupRanges, (r) -> r.encloses(currentChunkRange));
		return rollupRangesEnclosingChunk;
	}

	protected static String getRangeFileName(long groupingValue, long multiple) {
		Range timeRange = Range.forValueAndRangeSize(groupingValue, multiple);
		return timeRange.toUnderscoreDelimitedString();
	}

	private Path getPathFor(long groupingNumber, long rollupLevel) {
		String fileName = getRangeFileName(groupingNumber, rollupLevel);
		return Paths.get(segmentPath.toString(), fileName);
	}

	private BlueReadLock<Path> acquireReadLock(Path path) {
		return fileManager.getLockManager().acquireReadLock(path);
	}

	private BlueWriteLock<Path> acquireWriteLock(Path path) {
		return fileManager.getLockManager().acquireWriteLock(path);
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

	@Override
	public int compareTo(Segment<T> other) {
		return segmentRange.compareTo(other.segmentRange);
	}
}
