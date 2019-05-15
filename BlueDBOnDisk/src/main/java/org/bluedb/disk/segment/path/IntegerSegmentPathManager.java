package org.bluedb.disk.segment.path;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.bluedb.api.keys.BlueKey;

public class IntegerSegmentPathManager implements SegmentPathManager {

	private static final long SIZE_SEGMENT = 256;
	private static final long SIZE_FOLDER_BOTTOM = SIZE_SEGMENT * 64;
	private static final long SIZE_FOLDER_MIDDLE = SIZE_FOLDER_BOTTOM * 64;
	private static final long SIZE_FOLDER_TOP = SIZE_FOLDER_MIDDLE * 64;

	protected static final List<Long> DEFAULT_ROLLUP_LEVELS = Collections.unmodifiableList(Arrays.asList(1L, SIZE_SEGMENT));
	protected static final List<Long> DEFAULT_SIZE_FOLDERS  = Collections.unmodifiableList(Arrays.asList(SIZE_FOLDER_TOP, SIZE_FOLDER_MIDDLE, SIZE_FOLDER_BOTTOM, SIZE_SEGMENT));
	public static final long DEFAULT_SEGMENT_SIZE = DEFAULT_SIZE_FOLDERS.get(DEFAULT_SIZE_FOLDERS.size() - 1);

	private final Path collectionPath;
	private final List<Long> folderSizes;
	private final long segmentSize;
	private final List<Long> rollupLevels;

	public IntegerSegmentPathManager(Path collectionPath, long segmentSize) {
		this.collectionPath = collectionPath;
		this.folderSizes = DEFAULT_SIZE_FOLDERS;
		this.segmentSize = segmentSize;
		this.rollupLevels = DEFAULT_ROLLUP_LEVELS;
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
