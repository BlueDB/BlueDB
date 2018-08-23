package io.bluedb.disk.segment;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.HashGroupedKey;
import io.bluedb.api.keys.LongKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.file.FileManager;
import io.bluedb.disk.segment.path.LongSegmentPathManager;
import io.bluedb.disk.segment.path.IntegerSegmentPathManager;
import io.bluedb.disk.segment.path.SegmentPathManager;
import io.bluedb.disk.segment.path.TimeSegmentPathManager;
import io.bluedb.disk.segment.rollup.RollupScheduler;

public class SegmentManager<T extends Serializable> {


	private final SegmentPathManager pathManager;
	private final FileManager fileManager;
	private final RollupScheduler rollupScheduler;

	public SegmentManager(Path collectionPath, FileManager fileManager, RollupScheduler rollupScheduler, Class<? extends BlueKey> keyType) {
		this.fileManager = fileManager;
		this.rollupScheduler = rollupScheduler;
		this.pathManager = createSegmentPathManager(collectionPath, keyType);
	}

	public Range getSegmentRange(long groupingValue) {
		return Range.forValueAndRangeSize(groupingValue, getSegmentSize());
	}

	public Segment<T> getFirstSegment(BlueKey key) {
		long groupingNumber = key.getGroupingNumber();
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
		return new Segment<T>(path, range, this, fileManager, pathManager.getRollupLevels());
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

	public RollupScheduler getRollupScheduler() {
		return rollupScheduler;
	}

	protected static SegmentPathManager createSegmentPathManager(Path collectionPath, Class<? extends BlueKey> keyType) {
		if (TimeKey.class.isAssignableFrom(keyType)) {
			return new TimeSegmentPathManager(collectionPath);
		} else if (LongKey.class.isAssignableFrom(keyType)) {
			return new LongSegmentPathManager(collectionPath);
		} else if (HashGroupedKey.class.isAssignableFrom(keyType)) {
			return new IntegerSegmentPathManager(collectionPath);
		} else {
			throw new UnsupportedOperationException("Cannot create a SegmentPathManager for type " + keyType);
		}
	}
}
