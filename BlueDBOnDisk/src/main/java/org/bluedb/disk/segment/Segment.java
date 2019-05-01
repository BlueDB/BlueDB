package org.bluedb.disk.segment;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.Blutils.CheckedFunction;
import org.bluedb.disk.file.BlueObjectInput;
import org.bluedb.disk.file.BlueObjectOutput;
import org.bluedb.disk.file.FileManager;
import org.bluedb.disk.file.FileUtils;
import org.bluedb.disk.file.RangeNamedFiles;
import org.bluedb.disk.lock.BlueReadLock;
import org.bluedb.disk.lock.BlueWriteLock;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.segment.rollup.RollupTarget;
import org.bluedb.disk.segment.rollup.Rollupable;
import org.bluedb.disk.segment.writer.BatchWriter;
import org.bluedb.disk.segment.writer.DeleteWriter;
import org.bluedb.disk.segment.writer.InsertWriter;
import org.bluedb.disk.segment.writer.StreamingWriter;
import org.bluedb.disk.segment.writer.UpdateWriter;
import org.bluedb.disk.serialization.BlueEntity;

public class Segment <T extends Serializable> implements Comparable<Segment<T>> {

	private final Rollupable rollupable;
	private final FileManager fileManager;
	private final Path segmentPath;
	private final Range segmentRange;
	private final List<Long> rollupLevels;
	private final Range preSegmentRange;

	public Segment(Path segmentPath, Range segmentRange, Rollupable rollupable, FileManager fileManager, final List<Long> rollupLevels) {
		this.segmentPath = segmentPath;
		this.segmentRange = segmentRange;
		this.fileManager = fileManager;
		this.rollupLevels = rollupLevels;
		this.rollupable = rollupable;
		this.preSegmentRange = (segmentRange==null) ? null : new Range(0, segmentRange.getStart() - 1);
	}

	protected static <T extends Serializable> Segment<T> getTestSegment () {
		return new Segment<T>();
	}

	protected Segment() {segmentPath = null;segmentRange = null;fileManager = null;rollupLevels = null;rollupable = null;preSegmentRange = null;}

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
			tmpPath = FileUtils.createTempFilePath(targetPath);
			try(BlueObjectOutput<BlueEntity<T>> output = getObjectOutputFor(tmpPath)) {
				processor.process(input, output);
			}
		}

		try (BlueWriteLock<Path> targetFileLock = acquireWriteLock(targetPath)) {
			FileUtils.moveFile(tmpPath, targetFileLock);
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
		Path tmpPath = FileUtils.createTempFilePath(targetPath);
		BlueReadLock<Path> lock = acquireReadLock(targetPath);
		try (BlueObjectInput<BlueEntity<T>> input = fileManager.getBlueInputStream(lock)) {
			try(BlueObjectOutput<BlueEntity<T>> output = getObjectOutputFor(tmpPath)) {
				processor.process(input, output);
			}
		}
		try (BlueWriteLock<Path> targetFileLock = acquireWriteLock(targetPath)) {
			FileUtils.moveFile(tmpPath, targetFileLock);
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
		filesToRollup = filterAndDeleteEmptyFiles(filesToRollup);
		if (abortIfOnlyOneFile && filesToRollup.size() < 2) {
			return;  // no benefit to rolling up a single file
		}
		Path path = Paths.get(segmentPath.toString(), timeRange.toUnderscoreDelimitedString());
		Path tmpPath = FileUtils.createTempFilePath(path);

		if (path.toFile().exists()) { // we're recovering after a rollup failed while deleting the removed files
			filesToRollup = Blutils.filter(filesToRollup, (f) -> !f.equals(path.toFile()));  // don't delete the rolled up file
			cleanupFiles(filesToRollup);
		} else {
			copy(tmpPath, filesToRollup);
			moveRolledUpFileAndDeleteSourceFiles(path, tmpPath, filesToRollup);
		}
	}

	protected List<File> filterAndDeleteEmptyFiles(List<File> orderedFiles) {
		List<File> results = new ArrayList<>();
		for (File file: orderedFiles) {
			if (file.length() == 0) {
				fileManager.lockDeleteUnlock(file);
			} else {
				results.add(file);
			}
		}
		return results;
	}

	public boolean isValidRollupRange(Range timeRange) {
		if (preSegmentRange.equals(timeRange)) {
			return true;
		}
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
				FileUtils.deleteFile(writeLock);
			}
		}
	}

	private void moveRolledUpFileAndDeleteSourceFiles(Path newRolledupPath, Path tempRolledupPath, List<File> filesToRollup) throws BlueDbException {
		List<BlueWriteLock<Path>> sourceFileWriteLocks = new ArrayList<>();
		try (BlueWriteLock<Path> targetFileLock = acquireWriteLock(newRolledupPath)){
			for (File file: filesToRollup) {
				sourceFileWriteLocks.add(acquireWriteLock(file.toPath()));
			}

			FileUtils.moveFile(tempRolledupPath, targetFileLock);
			for (BlueWriteLock<Path> writeLock: sourceFileWriteLocks) {
				FileUtils.deleteFile(writeLock);
			}
		} finally {
			for (BlueWriteLock<Path> lock: sourceFileWriteLocks) {
				lock.release();
			}
		}
	}

	public static List<Range> getAllFileRangesInOrder(Path segmentPath) {
		File segmentFolder = segmentPath.toFile();
		List<File> allFilesInSegment = FileUtils.getFolderContents(segmentFolder);
		return allFilesInSegment.stream()
				.map( Range::fromFileWithUnderscoreDelmimitedName )
				.filter( Objects::nonNull )
				.sorted()
				.collect(Collectors.toList());
	}

	public List<File> getOrderedFilesEnclosedInRange(Range range) {
		return RangeNamedFiles.getOrderedFilesEnclosedInRange(segmentPath, range);
	}

	public List<File> getOrderedFilesInRange(Range range) {
		return RangeNamedFiles.getOrderedFilesInRange(segmentPath, range);
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
		if (groupingNumber < segmentRange.getStart()) {
			Path path = Paths.get(segmentPath.toString(), preSegmentRange.toUnderscoreDelimitedString());
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
		if (currentChunkRange.getEnd() < segmentRange.getStart()) {
			rollupRangesEnclosingChunk.add(preSegmentRange);
		}
		return rollupRangesEnclosingChunk;
	}

	private Path getPathFor(long groupingNumber, long rollupLevel) {
		String fileName = RangeNamedFiles.getRangeFileName(groupingNumber, rollupLevel);
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
