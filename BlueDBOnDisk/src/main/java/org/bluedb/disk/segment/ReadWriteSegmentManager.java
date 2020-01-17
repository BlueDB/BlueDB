package org.bluedb.disk.segment;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.file.ReadWriteFileManager;
import org.bluedb.disk.segment.path.SegmentSizeConfiguration;
import org.bluedb.disk.segment.rollup.Rollupable;

public class ReadWriteSegmentManager<T extends Serializable> extends ReadableSegmentManager<T> {

	private final ReadWriteFileManager fileManager;

	private final Rollupable rollupable;

	public ReadWriteSegmentManager(Path collectionPath, ReadWriteFileManager fileManager, Rollupable rollupable, SegmentSizeConfiguration sizeConfig) {
		super(collectionPath, sizeConfig);
		this.fileManager = fileManager;
		this.rollupable = rollupable;
	}

	protected ReadWriteSegment<T> toSegment(Path path) {
		Range range = toRange(path);
		return new ReadWriteSegment<T>(path, range, rollupable, getFileManager(), pathManager.getRollupLevels());
	}

	public ReadWriteFileManager getFileManager() {
		return fileManager;
	}

	@Override
	public ReadWriteSegment<T> getFirstSegment(BlueKey key) {
		long groupingNumber = key.getGroupingNumber();
		return getSegment(groupingNumber);
	}

	public ReadWriteSegment<T> getSegmentAfter(ReadWriteSegment<T> segment) {
		long groupingNumber = segment.getRange().getEnd() + 1;
		return getSegment(groupingNumber);
	}

	@Override
	public ReadWriteSegment<T> getSegment(long groupingNumber) {
		Path segmentPath = pathManager.getSegmentPath(groupingNumber);
		return toSegment(segmentPath);
	}

	public List<ReadWriteSegment<T>> getAllSegments(BlueKey key) {
		return pathManager.getAllPossibleSegmentPaths(key).stream()
				.map((p) -> (toSegment(p)))
				.collect(Collectors.toList());
	}

	@Override
	public List<ReadWriteSegment<T>> getAllExistingSegments() {
		Range allValues = new Range(Long.MIN_VALUE, Long.MAX_VALUE);
		return getExistingSegments(allValues);
	}

	@Override
	public List<ReadWriteSegment<T>> getExistingSegments(Range range) {
		return pathManager.getExistingSegmentFiles(range).stream()
				.map((f) -> (toSegment(f.toPath())))
				.sorted()
				.collect(Collectors.toList());
	}


}
