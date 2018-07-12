package io.bluedb.disk.segment;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.file.FileManager;

public class SegmentManager<T extends Serializable> {

	protected static final long SIZE_SEGMENT = TimeUnit.HOURS.toMillis(1);
	protected static final long SIZE_FOLDER_BOTTOM = SIZE_SEGMENT * 24;
	protected static final long SIZE_FOLDER_MIDDLE = SIZE_FOLDER_BOTTOM * 30;
	protected static final long SIZE_FOLDER_TOP = SIZE_FOLDER_MIDDLE * 12;

	private final BlueCollectionOnDisk<T> collection;
	private final FileManager fileManager;
	
	public SegmentManager(BlueCollectionOnDisk<T> collection) {
		this.collection = collection;
		this.fileManager = collection.getFileManager();
	}

	public static Range getSegmentTimeRange(long groupingValue) {
		return getTimeRange(groupingValue, getSegmentSize());
	}

	public static Range getTimeRange(long groupingValue, long multiple) {
		long low = Blutils.roundDownToMultiple(groupingValue, multiple);
		long high = Math.min(Long.MAX_VALUE - multiple + 1, low) + multiple - 1;  // prevent overflow
		return new Range(low, high);
	}

	public Segment<T> getFirstSegment(BlueKey key) {
		long groupingNumber = key.getGroupingNumber();
		return getSegment(groupingNumber);
	}

	public Segment<T> getSegment(long groupingNumber) {
		Path segmentPath = getPath(groupingNumber);
		return toSegment(segmentPath);
	}

	public List<Segment<T>> getAllSegments(BlueKey key) {
		return getAllPossibleSegmentPaths(key).stream()
				.map((p) -> (toSegment(p)))
				.collect(Collectors.toList());
	}

	public List<Segment<T>> getExistingSegments(long minValue, long maxValue) {
		return getExistingSegmentFiles(minValue, maxValue).stream()
				.map((f) -> (toSegment(f.toPath())))
				.collect(Collectors.toList());
	}

	protected Path getPath(BlueKey key) {
		long groupingNumber = key.getGroupingNumber();
		return getPath(groupingNumber);
	}

	protected Path getPath(long groupingNumber) {
		return Paths.get(
				collection.getPath().toString(),
				String.valueOf(groupingNumber / SIZE_FOLDER_TOP),
				String.valueOf(groupingNumber / SIZE_FOLDER_MIDDLE),
				String.valueOf(groupingNumber / SIZE_FOLDER_BOTTOM),
				String.valueOf(groupingNumber / SIZE_SEGMENT));
	}

	protected List<Path> getAllPossibleSegmentPaths(BlueKey key) {
		if (key instanceof TimeFrameKey) {
			TimeFrameKey timeFrameKey = (TimeFrameKey)key;
			return getAllPossibleSegmentPaths(timeFrameKey.getStartTime(), timeFrameKey.getEndTime());
		} else {
			Path path = getPath(key);
			return Arrays.asList(path);
		}
	}

	protected List<Path> getAllPossibleSegmentPaths(long minTime, long maxTime) {
		List<Path> paths = new ArrayList<>();
		minTime = minTime - (minTime % SIZE_SEGMENT);
		long i = minTime;
		while (i <= maxTime) {
			Path path = getPath(i);
			paths.add(path);
			i += SIZE_SEGMENT;
		}
		return paths;
	}

	protected List<File> getExistingSegmentFiles(long minValue, long maxValue) {
		File collectionFolder = collection.getPath().toFile();
		List<File> topLevelFolders = getNonsegmentSubfoldersInRange(collectionFolder, minValue/SIZE_FOLDER_TOP, maxValue/SIZE_FOLDER_TOP);
		List<File> midLevelFolders = getNonsegmentSubfoldersInRange(topLevelFolders, minValue/SIZE_FOLDER_MIDDLE, maxValue/SIZE_FOLDER_MIDDLE);
		List<File> bottomLevelFolders = getNonsegmentSubfoldersInRange(midLevelFolders, minValue/SIZE_FOLDER_BOTTOM, maxValue/SIZE_FOLDER_BOTTOM);
		List<File> segmentFolders = getNonsegmentSubfoldersInRange(bottomLevelFolders, minValue/SIZE_SEGMENT, maxValue/SIZE_SEGMENT);
		return segmentFolders;
	}

	protected Segment<T> toSegment(Path path) {
		return new Segment<T>(path, fileManager);
	}

	public static long getSegmentSize() {
		return SIZE_SEGMENT;
	}

	protected static List<File> getNonsegmentSubfoldersInRange(File folder, long minValue, long maxValue) {
		return FileManager.getFolderContents(folder)
            .stream()
			.filter((f) -> f.isDirectory())
			.filter((f) -> folderNameIsLongInRange(f, minValue, maxValue))
			.collect(Collectors.toList());
	}

	protected static List<File> getNonsegmentSubfoldersInRange(List<File> folders, long minValue, long maxValue) {
		List<File> results = new ArrayList<>();
		for (File folder: folders) {
			results.addAll(getNonsegmentSubfoldersInRange(folder, minValue, maxValue));
		}
		return results;
	}
	
	protected static boolean folderNameIsLongInRange(File file, long minValue, long maxValue) {
		try {
			long fileNameAsLong = Long.valueOf(file.getName());
			return fileNameAsLong >= minValue && fileNameAsLong <= maxValue;
		} catch(Exception e) {
			return false;
		}
	}
	protected static String getRangeFileName(long groupingValue, long multiple) {
		Range timeRange = getTimeRange(groupingValue, multiple);
		return timeRange.toUnderscoreDelimitedString();
	}

}
