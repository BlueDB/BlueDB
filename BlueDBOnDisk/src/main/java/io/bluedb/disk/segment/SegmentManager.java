package io.bluedb.disk.segment;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.api.keys.ValueKey;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.segment.path.SegmentPathManager;
import io.bluedb.disk.segment.path.TimeSegmentPathManager;

public class SegmentManager<T extends Serializable> {


	private final BlueCollectionOnDisk<T> collection;
	private final SegmentPathManager pathManager;

	public SegmentManager(BlueCollectionOnDisk<T> collection, Class<? extends BlueKey> keyType) {
		this.collection = collection;
		Path collectionPath = collection.getPath();
		this.pathManager =createSegmentPathManager(collectionPath, keyType);
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

	public List<Segment<T>> getExistingSegments(long minValue, long maxValue) {
		return pathManager.getExistingSegmentFiles(minValue, maxValue).stream()
				.map((f) -> (toSegment(f.toPath())))
				.collect(Collectors.toList());
	}

	public SegmentPathManager getPathManager() {
		return pathManager;
	}

	protected Segment<T> toSegment(Path path) {
		return new Segment<T>(path, collection, pathManager.getRollupLevels());
	}

	public long getSegmentSize() {
		return pathManager.getSegmentSize();
	}

	protected static SegmentPathManager createSegmentPathManager(Path collectionPath, Class<? extends BlueKey> keyType) {
		if (TimeKey.class.isAssignableFrom(keyType)) {
			return new TimeSegmentPathManager(collectionPath);
		} else if (ValueKey.class.isAssignableFrom(keyType)) {
			return new TimeSegmentPathManager(collectionPath);
		} else {
			throw new UnsupportedOperationException("Cannot create a SegmentPathManager for type " + keyType);
		}
	}
}
