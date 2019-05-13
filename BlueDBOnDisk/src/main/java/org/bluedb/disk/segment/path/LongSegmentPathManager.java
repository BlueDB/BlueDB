package org.bluedb.disk.segment.path;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.bluedb.api.keys.BlueKey;

public class LongSegmentPathManager implements SegmentPathManager {

	private static final long SIZE_SEGMENT = 64;
	private static final long SIZE_FOLDER_LOWER_BOTTOM = SIZE_SEGMENT * 256;
	private static final long SIZE_FOLDER_LOWER_MIDDLE = SIZE_FOLDER_LOWER_BOTTOM * 512;
	private static final long SIZE_FOLDER_LOWER_TOP = SIZE_FOLDER_LOWER_MIDDLE * 512;
	private static final long SIZE_FOLDER_UPPER_BOTTOM = SIZE_FOLDER_LOWER_TOP * 512;
	private static final long SIZE_FOLDER_UPPER_MIDDLE = SIZE_FOLDER_UPPER_BOTTOM * 256;
	private static final long SIZE_FOLDER_UPPER_TOP = SIZE_FOLDER_UPPER_MIDDLE * 128;

	protected static final List<Long> DEFAULT_ROLLUP_LEVELS = Collections.unmodifiableList(Arrays.asList(1L, SIZE_SEGMENT));
	protected static final List<Long> DEFAULT_SIZE_FOLDERS = Collections.unmodifiableList(Arrays.asList(
			SIZE_FOLDER_UPPER_TOP, SIZE_FOLDER_UPPER_MIDDLE, SIZE_FOLDER_UPPER_BOTTOM,
			SIZE_FOLDER_LOWER_TOP, SIZE_FOLDER_LOWER_MIDDLE, SIZE_FOLDER_LOWER_BOTTOM,
			SIZE_SEGMENT
			));

	private final Path collectionPath;
	private final List<Long> folderSizes;
	private final long segmentSize;
	private final List<Long> rollupLevels;

	public LongSegmentPathManager(Path collectionPath) {
		this.collectionPath = collectionPath;
		this.folderSizes = DEFAULT_SIZE_FOLDERS;
		this.segmentSize = this.folderSizes.get(this.folderSizes.size() - 1);
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
