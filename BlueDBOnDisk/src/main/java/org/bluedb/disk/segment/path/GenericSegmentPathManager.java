package org.bluedb.disk.segment.path;

import java.nio.file.Path;
import java.util.List;

import org.bluedb.api.keys.BlueKey;

public class GenericSegmentPathManager implements SegmentPathManager {

	private final Path collectionPath;
	private final List<Long> folderSizes;
	private final long segmentSize;
	private final List<Long> rollupLevels;

	public GenericSegmentPathManager(Path collectionPath, long segmentSize, List<Long> folderSizes, List<Long> rollupLevels) {
		this.collectionPath = collectionPath;
		this.folderSizes = folderSizes;
		this.segmentSize = segmentSize;
		this.rollupLevels = rollupLevels;
	}

	@Override
	public Path getSegmentPath(BlueKey key) {
		long groupingNumber = key.getGroupingNumber();
		return getSegmentPath(groupingNumber);
	}

	@Override
	public long getSegmentSize() {
		return segmentSize;
	}

	@Override
	public List<Long> getRollupLevels() {
		return rollupLevels;
	}

	@Override
	public List<Long> getFolderSizes() {
		return folderSizes;
	}

	@Override
	public Path getCollectionPath() {
		return collectionPath;
	}
}
