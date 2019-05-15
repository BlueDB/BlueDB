package org.bluedb.disk.segment;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.HashGroupedKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.file.FileManager;
import org.bluedb.disk.segment.path.LongSegmentPathManager;
import org.bluedb.disk.segment.path.GenericSegmentPathManager;
import org.bluedb.disk.segment.path.HashSegmentPathManager;
import org.bluedb.disk.segment.path.IntegerSegmentPathManager;
import org.bluedb.disk.segment.path.SegmentPathManager;
import org.bluedb.disk.segment.path.TimeSegmentPathManager;
import org.bluedb.disk.segment.rollup.Rollupable;

public class SegmentManager<T extends Serializable> {


	private final SegmentPathManager pathManager;
	private final FileManager fileManager;
	private final Rollupable rollupable;

	public SegmentManager(Path collectionPath, FileManager fileManager, Rollupable rollupable, Class<? extends BlueKey> keyType) {
		this.fileManager = fileManager;
		this.rollupable = rollupable;
		this.pathManager = createSegmentPathManager(collectionPath, keyType);
	}

	public Range getSegmentRange(long groupingValue) {
		return Range.forValueAndRangeSize(groupingValue, getSegmentSize());
	}

	public Segment<T> getFirstSegment(BlueKey key) {
		long groupingNumber = key.getGroupingNumber();
		return getSegment(groupingNumber);
	}

	public Segment<T> getSegmentAfter(Segment<T> segment) {
		long groupingNumber = segment.getRange().getEnd() + 1;
		return getSegment(groupingNumber);
	}

	public Segment<T> getSegment(long groupingNumber) {
		Path segmentPath = pathManager.getSegmentPath(groupingNumber);
		return toSegment(segmentPath);
	}

	public List<Segment<T>> getAllSegments(BlueKey key) {
		return pathManager.getAllPossibleSegmentPaths(key).stream()
				.map((p) -> (toSegment(p)))
				.collect(Collectors.toList());
	}

	public List<Segment<T>> getAllExistingSegments() {
		Range allValues = new Range(Long.MIN_VALUE, Long.MAX_VALUE);
		return getExistingSegments(allValues);
	}

	public List<Segment<T>> getExistingSegments(Range range) {
		return pathManager.getExistingSegmentFiles(range).stream()
				.map((f) -> (toSegment(f.toPath())))
				.sorted()
				.collect(Collectors.toList());
	}

	public SegmentPathManager getPathManager() {
		return pathManager;
	}

	protected Segment<T> toSegment(Path path) {
		Range range = toRange(path);
		return new Segment<T>(path, range, rollupable, fileManager, pathManager.getRollupLevels());
	}

	public Range toRange(Path path) {
		String filename = path.toFile().getName();
		long rangeStart = Long.valueOf(filename) * getSegmentSize();
		Range range = new Range(rangeStart, rangeStart + getSegmentSize() - 1);
		return range;
	}

	public long getSegmentSize() {
		return pathManager.getSegmentSize();
	}

	protected static SegmentPathManager createSegmentPathManager(Path collectionPath, Class<? extends BlueKey> keyType) {
		if (TimeKey.class.isAssignableFrom(keyType)) {
			long segmentSize = TimeSegmentPathManager.DEFAULT_SEGMENT_SIZE;
			List<Long> folderSizes = TimeSegmentPathManager.DEFAULT_SIZE_FOLDERS;
			List<Long> rollupSizes = TimeSegmentPathManager.DEFAULT_ROLLUP_LEVELS;
			return new GenericSegmentPathManager(collectionPath, segmentSize, folderSizes, rollupSizes);
		} else if (LongKey.class.isAssignableFrom(keyType)) {
			long segmentSize = LongSegmentPathManager.DEFAULT_SEGMENT_SIZE;
			List<Long> folderSizes = LongSegmentPathManager.DEFAULT_SIZE_FOLDERS;
			List<Long> rollupSizes = LongSegmentPathManager.DEFAULT_ROLLUP_LEVELS;
			return new GenericSegmentPathManager(collectionPath, segmentSize, folderSizes, rollupSizes);
		} else if (IntegerKey.class.isAssignableFrom(keyType)) {
			long segmentSize = IntegerSegmentPathManager.DEFAULT_SEGMENT_SIZE;
			List<Long> folderSizes = IntegerSegmentPathManager.DEFAULT_SIZE_FOLDERS;
			List<Long> rollupSizes = IntegerSegmentPathManager.DEFAULT_ROLLUP_LEVELS;
			return new GenericSegmentPathManager(collectionPath, segmentSize, folderSizes, rollupSizes);
		} else if (HashGroupedKey.class.isAssignableFrom(keyType)) {
			long segmentSize = HashSegmentPathManager.DEFAULT_SEGMENT_SIZE;
			List<Long> folderSizes = HashSegmentPathManager.DEFAULT_SIZE_FOLDERS;
			List<Long> rollupSizes = HashSegmentPathManager.DEFAULT_ROLLUP_LEVELS;
			return new GenericSegmentPathManager(collectionPath, segmentSize, folderSizes, rollupSizes);
		} else {
			throw new UnsupportedOperationException("Cannot create a SegmentPathManager for type " + keyType);
		}
	}
}
