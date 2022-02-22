package org.bluedb.disk.segment;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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

	@Override
	public ReadOnlySegment<T> getFirstSegment(BlueKey key) {
		long groupingNumber = key.getGroupingNumber();
		return getSegment(groupingNumber);
	}

	@Override
	public ReadOnlySegment<T> getSegment(long groupingNumber) {
		Path segmentPath = pathManager.getSegmentPath(groupingNumber);
		return toSegment(segmentPath);
	}

	@Override
	public List<ReadOnlySegment<T>> getAllExistingSegments() {
		Range allValues = new Range(Long.MIN_VALUE, Long.MAX_VALUE);
		return getExistingSegments(allValues, Optional.empty());
	}

	@Override
	public List<ReadOnlySegment<T>> getExistingSegments(Range range, Optional<Set<Range>> segmentRangesToInclude) {
		return pathManager.getExistingSegmentFiles(range).stream()
				.map((f) -> (toSegment(f.toPath())))
				.filter(s -> !segmentRangesToInclude.isPresent() || segmentRangesToInclude.get().contains(s.getRange()))
				.sorted()
				.collect(Collectors.toList());
	}
}
