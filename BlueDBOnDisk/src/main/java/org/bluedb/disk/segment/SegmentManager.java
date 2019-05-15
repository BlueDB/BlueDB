package org.bluedb.disk.segment;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.HashGroupedKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.file.FileManager;
import org.bluedb.disk.segment.path.SegmentPathManager;
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

	private static final long HOUR = TimeUnit.HOURS.toMillis(1);

	public final static List<Long> DEFAULT_ROLLUP_LEVELS_TIME = unmodifiableList(1L, 6000L, HOUR);
	public final static List<Long> DEFAULT_SIZE_FOLDERS_TIME = createMultiples(HOUR, 24L, 30L, 12L);

	private static final long SIZE_SEGMENT_HASH = 524288;
	
	public static final List<Long> DEFAULT_ROLLUP_LEVELS_HASH = unmodifiableList(1L, SIZE_SEGMENT_HASH);
	public static final List<Long> DEFAULT_SIZE_FOLDERS_HASH = createMultiples(SIZE_SEGMENT_HASH, 128L, 64L);

	private static final long SEGMENT_SIZE_INT = 256;

	public static final List<Long> DEFAULT_ROLLUP_LEVELS_INTEGER = unmodifiableList(1L, SEGMENT_SIZE_INT);
	public static final List<Long> DEFAULT_SIZE_FOLDERS_INTEGER  = createMultiples(SEGMENT_SIZE_INT, 64L, 64L, 64L);

	
	private static final long SIZE_SEGMENT_LONG = 64;

	public static final List<Long> DEFAULT_ROLLUP_LEVELS_LONG = unmodifiableList(1L, SIZE_SEGMENT_LONG);
	public static final List<Long> DEFAULT_SIZE_FOLDERS_LONG  = createMultiples(SIZE_SEGMENT_LONG, 256L, 512L, 512L, 512L, 256L, 128L);
	
	@SafeVarargs
	public static <T> List<T> unmodifiableList(T ...values) {
		return Collections.unmodifiableList(Arrays.asList(values));
	}

	public static List<Long> createMultiples(Long ...bottomToTopWidths) {
		List<Long> result = new ArrayList<>();
		long accumulator = 1;
		for (long iterValue: bottomToTopWidths) {
			accumulator *= iterValue;
			result.add(accumulator);
		}
		Collections.reverse(result);
		return result;
	}

	protected static SegmentPathManager createSegmentPathManager(Path collectionPath, Class<? extends BlueKey> keyType) {
		List<Long> folderSizes;
		List<Long> rollupSizes;
		if (TimeKey.class.isAssignableFrom(keyType)) {
			folderSizes = DEFAULT_SIZE_FOLDERS_TIME;
			rollupSizes = DEFAULT_ROLLUP_LEVELS_TIME;
		} else if (LongKey.class.isAssignableFrom(keyType)) {
			folderSizes = DEFAULT_SIZE_FOLDERS_LONG;
			rollupSizes = DEFAULT_ROLLUP_LEVELS_LONG;
		} else if (IntegerKey.class.isAssignableFrom(keyType)) {
			folderSizes = DEFAULT_SIZE_FOLDERS_INTEGER;
			rollupSizes = DEFAULT_ROLLUP_LEVELS_INTEGER;
		} else if (HashGroupedKey.class.isAssignableFrom(keyType)) {
			folderSizes = DEFAULT_SIZE_FOLDERS_HASH;
			rollupSizes = DEFAULT_ROLLUP_LEVELS_HASH;
		} else {
			throw new UnsupportedOperationException("Cannot create a SegmentPathManager for type " + keyType);
		}
		long segmentSize = folderSizes.get(folderSizes.size() - 1);
		return new SegmentPathManager(collectionPath, segmentSize, folderSizes, rollupSizes);
	}
}
