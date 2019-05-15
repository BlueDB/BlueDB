package org.bluedb.disk.segment.path;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TimeSegmentPathManager {

	private static final long DEFAULT_SIZE_SEGMENT = TimeUnit.HOURS.toMillis(1);
	private static final long SIZE_FOLDER_BOTTOM = DEFAULT_SIZE_SEGMENT * 24;
	private static final long SIZE_FOLDER_MIDDLE = SIZE_FOLDER_BOTTOM * 30;
	private static final long SIZE_FOLDER_TOP = SIZE_FOLDER_MIDDLE * 12;

	public final static List<Long> DEFAULT_ROLLUP_LEVELS = Collections.unmodifiableList(Arrays.asList(1L, 6000L, DEFAULT_SIZE_SEGMENT));
	public final static List<Long> DEFAULT_SIZE_FOLDERS = Collections.unmodifiableList(Arrays.asList(SIZE_FOLDER_TOP, SIZE_FOLDER_MIDDLE, SIZE_FOLDER_BOTTOM, DEFAULT_SIZE_SEGMENT));
	public static final long DEFAULT_SEGMENT_SIZE = DEFAULT_SIZE_FOLDERS.get(DEFAULT_SIZE_FOLDERS.size() - 1);
}
