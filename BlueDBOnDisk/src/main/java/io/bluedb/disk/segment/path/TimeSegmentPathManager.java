package io.bluedb.disk.segment.path;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import io.bluedb.api.keys.BlueKey;

public class TimeSegmentPathManager implements SegmentPathManager {

	public static final long SIZE_SEGMENT = TimeUnit.HOURS.toMillis(1);
	public static final long SIZE_FOLDER_BOTTOM = SIZE_SEGMENT * 24;
	public static final long SIZE_FOLDER_MIDDLE = SIZE_FOLDER_BOTTOM * 30;
	public static final long SIZE_FOLDER_TOP = SIZE_FOLDER_MIDDLE * 12;
	public final static List<Long> ROLLUP_LEVELS = Collections.unmodifiableList(Arrays.asList(1L, SIZE_SEGMENT));

	private final Path collectionPath;

	public TimeSegmentPathManager(Path collectionPath) {
		this.collectionPath = collectionPath;
	}

	@Override
	public Path getSegmentPath(BlueKey key) {
		long groupingNumber = key.getGroupingNumber();
		return getSegmentPath(groupingNumber);
	}

	@Override
	public Path getSegmentPath(long groupingNumber) {
		return Paths.get(
				collectionPath.toString(),
				String.valueOf(groupingNumber / SIZE_FOLDER_TOP),
				String.valueOf(groupingNumber / SIZE_FOLDER_MIDDLE),
				String.valueOf(groupingNumber / SIZE_FOLDER_BOTTOM),
				String.valueOf(groupingNumber / SIZE_SEGMENT));
	}

	@Override
	public List<Path> getAllPossibleSegmentPaths(BlueKey key) {
		List<Path> paths = new ArrayList<>();
		long minTime = key.getGroupingNumber();
		minTime = minTime - (minTime % SIZE_SEGMENT);
		long i = minTime;
		while (key.isInRange(i, i + SIZE_SEGMENT - 1)) {
			Path path = getSegmentPath(i);
			paths.add(path);
			i += SIZE_SEGMENT;
		}
		return paths;
	}

	@Override
	public List<File> getExistingSegmentFiles(long minValue, long maxValue) {
		File collectionFolder = collectionPath.toFile();
		List<File> topLevelFolders = SegmentPathManager.getSubfoldersInRange(collectionFolder, minValue/SIZE_FOLDER_TOP, maxValue/SIZE_FOLDER_TOP);
		List<File> midLevelFolders = SegmentPathManager.getSubfoldersInRange(topLevelFolders, minValue/SIZE_FOLDER_MIDDLE, maxValue/SIZE_FOLDER_MIDDLE);
		List<File> bottomLevelFolders = SegmentPathManager.getSubfoldersInRange(midLevelFolders, minValue/SIZE_FOLDER_BOTTOM, maxValue/SIZE_FOLDER_BOTTOM);
		List<File> segmentFolders = SegmentPathManager.getSubfoldersInRange(bottomLevelFolders, minValue/SIZE_SEGMENT, maxValue/SIZE_SEGMENT);
		return segmentFolders;
	}

	@Override
	public long getSegmentSize() {
		return SIZE_SEGMENT;
	}

	@Override
	public List<Long> getRollupLevels() {
		return ROLLUP_LEVELS;
	}
}
