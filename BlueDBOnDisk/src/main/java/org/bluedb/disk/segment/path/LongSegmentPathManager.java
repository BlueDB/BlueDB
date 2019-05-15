package org.bluedb.disk.segment.path;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LongSegmentPathManager {

	private static final long SIZE_SEGMENT = 64;
	private static final long SIZE_FOLDER_LOWER_BOTTOM = SIZE_SEGMENT * 256;
	private static final long SIZE_FOLDER_LOWER_MIDDLE = SIZE_FOLDER_LOWER_BOTTOM * 512;
	private static final long SIZE_FOLDER_LOWER_TOP = SIZE_FOLDER_LOWER_MIDDLE * 512;
	private static final long SIZE_FOLDER_UPPER_BOTTOM = SIZE_FOLDER_LOWER_TOP * 512;
	private static final long SIZE_FOLDER_UPPER_MIDDLE = SIZE_FOLDER_UPPER_BOTTOM * 256;
	private static final long SIZE_FOLDER_UPPER_TOP = SIZE_FOLDER_UPPER_MIDDLE * 128;

	public static final List<Long> DEFAULT_ROLLUP_LEVELS = Collections.unmodifiableList(Arrays.asList(1L, SIZE_SEGMENT));
	public static final List<Long> DEFAULT_SIZE_FOLDERS = Collections.unmodifiableList(Arrays.asList(
			SIZE_FOLDER_UPPER_TOP, SIZE_FOLDER_UPPER_MIDDLE, SIZE_FOLDER_UPPER_BOTTOM,
			SIZE_FOLDER_LOWER_TOP, SIZE_FOLDER_LOWER_MIDDLE, SIZE_FOLDER_LOWER_BOTTOM,
			SIZE_SEGMENT
			));
	public static final long DEFAULT_SEGMENT_SIZE = DEFAULT_SIZE_FOLDERS.get(DEFAULT_SIZE_FOLDERS.size() - 1);
}
