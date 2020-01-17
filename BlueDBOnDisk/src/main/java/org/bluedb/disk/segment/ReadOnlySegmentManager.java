package org.bluedb.disk.segment;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.file.ReadOnlyFileManager;
import org.bluedb.disk.segment.path.SegmentSizeConfiguration;

public class ReadOnlySegmentManager<T extends Serializable> extends ReadableSegmentManager<T> {

	private final ReadOnlyFileManager fileManager;

	public ReadOnlySegmentManager(Path collectionPath, ReadOnlyFileManager fileManager, SegmentSizeConfiguration sizeConfig) {
		super(collectionPath, sizeConfig);
		this.fileManager = fileManager;
	}

	protected ReadOnlySegment<T> toSegment(Path path) {
		Range range = toRange(path);
		return new ReadOnlySegment<T>(path, range, fileManager, pathManager.getRollupLevels());
	}

	public ReadOnlyFileManager getFileManager() {
		return fileManager;
	}

	@Override
	public ReadOnlySegment<T> getFirstSegment(BlueKey key) {
		long groupingNumber = key.getGroupingNumber();
		return getSegment(groupingNumber);
	}

	@Override
	public ReadOnlySegment<T> getSegmentAfter(Segment<T> segment) {
		long groupingNumber = segment.getRange().getEnd() + 1;
		return getSegment(groupingNumber);
	}

	@Override
	public ReadOnlySegment<T> getSegment(long groupingNumber) {
		Path segmentPath = pathManager.getSegmentPath(groupingNumber);
		return toSegment(segmentPath);
	}

	@Override
	public List<ReadOnlySegment<T>> getAllSegments(BlueKey key) {
		return pathManager.getAllPossibleSegmentPaths(key).stream()
				.map((p) -> (toSegment(p)))
				.collect(Collectors.toList());
	}

	@Override
	public List<ReadOnlySegment<T>> getAllExistingSegments() {
		Range allValues = new Range(Long.MIN_VALUE, Long.MAX_VALUE);
		return getExistingSegments(allValues);
	}

	@Override
	public List<ReadOnlySegment<T>> getExistingSegments(Range range) {
		return pathManager.getExistingSegmentFiles(range).stream()
				.map((f) -> (toSegment(f.toPath())))
				.sorted()
				.collect(Collectors.toList());
	}
}
