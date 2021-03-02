package org.bluedb.disk.segment;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.file.BlueObjectInput;
import org.bluedb.disk.file.FileUtils;
import org.bluedb.disk.file.RangeNamedFiles;
import org.bluedb.disk.file.ReadFileManager;
import org.bluedb.disk.lock.BlueReadLock;
import org.bluedb.disk.serialization.BlueEntity;

public abstract class ReadableSegment <T extends Serializable> implements Comparable<ReadableSegment<T>> {

	protected final Path segmentPath;
	protected final Range segmentRange;
	protected final List<Range> preSegmentRanges;
	protected final List<Long> rollupLevels;

	public ReadableSegment(Path segmentPath, Range segmentRange, final List<Long> rollupLevels) {
		this.segmentPath = segmentPath;
		this.segmentRange = segmentRange;
		this.rollupLevels = rollupLevels;
		
		this.preSegmentRanges = new LinkedList<>();
		if(segmentRange != null) {
			long preSegmentEnd = segmentRange.getStart() - 1;
			if(preSegmentEnd > 0) {
				this.preSegmentRanges.add(new Range(Long.MIN_VALUE, -1)); //Negative values before segment
				this.preSegmentRanges.add(new Range(0, preSegmentEnd)); //Positive values before segment
			}
			this.preSegmentRanges.add(new Range(Long.MIN_VALUE, preSegmentEnd)); //Always want a range that covers the entire pre-segment
		}
	}

	protected abstract ReadFileManager getFileManager();

	@Override
	public String toString() {
		return "<Segment for path " + segmentPath.toString() + ">";
	}

	public boolean contains(BlueKey key) throws BlueDbException {
		return get(key) != null;
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
		return new SegmentEntityIterator<T>(this, min, max);
	}


	public static List<Range> getAllFileRangesInOrder(Path segmentPath) {
		File segmentFolder = segmentPath.toFile();
		List<File> allFilesInSegment = FileUtils.getFolderContentsExcludingTempFiles(segmentFolder);
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

	protected BlueObjectInput<BlueEntity<T>> getObjectInputFor(Path path) throws BlueDbException {
		BlueReadLock<Path> lock = acquireReadLock(path);
		return getFileManager().getBlueInputStream(lock);
	}

	public BlueObjectInput<BlueEntity<T>> getObjectInputFor(long groupingNumber) throws BlueDbException {
		BlueReadLock<Path> lock = getReadLockFor(groupingNumber);
		return getFileManager().getBlueInputStream(lock);
	}

	public BlueReadLock<Path> getReadLockFor(long groupingNumber) throws BlueDbException {
		Path path = getPathFor(groupingNumber);
		return acquireReadLock(path);
	}
	
	public Path getPathFor(long groupingNumber) throws BlueDbException {
		for (long rollupLevel: rollupLevels) {
			Path path = getPathFor(groupingNumber, rollupLevel);
			if(FileUtils.exists(path)) {
				return path;
			}
		}
		
		for(Range preSegmentRange : preSegmentRanges) {
			if(preSegmentRange.containsInclusive(groupingNumber)) {
				Path path = getPathFor(preSegmentRange);
				if(FileUtils.exists(path)) {
					return path;
				}
			}
		}
		
		return getPathFor(groupingNumber, 1);
	}

	public Path getPath() {
		return segmentPath;
	}

	public List<Range> calculatePossibleChunkRanges(long groupingNumber) {
		List<Range> chunkRanges = rollupLevels.stream()
				.map( (rangeSize) -> Range.forValueAndRangeSize(groupingNumber, rangeSize))
				.collect(Collectors.toList());
		
		for(Range preSegmentRange : preSegmentRanges) {
			if(preSegmentRange.containsInclusive(groupingNumber)) {
				chunkRanges.add(preSegmentRange);
			}
		}
		
		return chunkRanges;
	}

	private Path getPathFor(Range range) {
		return Paths.get(segmentPath.toString(), range.toUnderscoreDelimitedString());
	}

	private Path getPathFor(long groupingNumber, long rollupLevel) {
		String fileName = RangeNamedFiles.getRangeFileName(groupingNumber, rollupLevel);
		return Paths.get(segmentPath.toString(), fileName);
	}

	protected BlueReadLock<Path> acquireReadLock(Path path) {
		return getFileManager().getLockManager().acquireReadLock(path);
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
	
	public boolean isValidPreSegmentRange(Range range) {
		for(Range preSegmentRange : preSegmentRanges) {
			if(preSegmentRange.equals(range)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public int hashCode() {
		return 31 + ((segmentPath == null) ? 0 : segmentPath.hashCode());
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof ReadableSegment)) {
			return false;
		}
		ReadableSegment<?> other = (ReadableSegment<?>) obj;
		if (segmentPath == null) {
			return other.segmentPath == null;
		} else {
			return segmentPath.equals(other.segmentPath);
		}
	}

	@Override
	public int compareTo(ReadableSegment<T> other) {
		return segmentRange.compareTo(other.segmentRange);
	}
}
