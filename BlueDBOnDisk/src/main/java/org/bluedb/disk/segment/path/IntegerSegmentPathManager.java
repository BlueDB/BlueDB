package org.bluedb.disk.segment.path;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class IntegerSegmentPathManager {

	private static final long SIZE_SEGMENT = 256;
	private static final long SIZE_FOLDER_BOTTOM = SIZE_SEGMENT * 64;
	private static final long SIZE_FOLDER_MIDDLE = SIZE_FOLDER_BOTTOM * 64;
	private static final long SIZE_FOLDER_TOP = SIZE_FOLDER_MIDDLE * 64;

	public static final List<Long> DEFAULT_ROLLUP_LEVELS = Collections.unmodifiableList(Arrays.asList(1L, SIZE_SEGMENT));
	public static final List<Long> DEFAULT_SIZE_FOLDERS  = Collections.unmodifiableList(Arrays.asList(SIZE_FOLDER_TOP, SIZE_FOLDER_MIDDLE, SIZE_FOLDER_BOTTOM, SIZE_SEGMENT));
	public static final long DEFAULT_SEGMENT_SIZE = DEFAULT_SIZE_FOLDERS.get(DEFAULT_SIZE_FOLDERS.size() - 1);
}
