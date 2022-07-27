package org.bluedb.disk.segment.path;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.file.FileUtils;
import org.bluedb.disk.segment.Range;


public class SegmentPathManager {

	private final Path collectionPath;
	private final List<Long> folderSizes;
	private final long segmentSize;
	private final List<Long> rollupLevels;

	public SegmentPathManager(Path collectionPath, SegmentSizeConfiguration sizeConfig) {
		this.collectionPath = collectionPath;
		
		this.folderSizes = sizeConfig.getFolderSizesTopToBottom();
		this.segmentSize = sizeConfig.getSegmentSize();
		this.rollupLevels = sizeConfig.getRollupsBottomToTop();
	}

	public Path getSegmentPath(BlueKey key) {
		long groupingNumber = key.getGroupingNumber();
		return getSegmentPath(groupingNumber);
	}

	public long getSegmentSize() {
		return segmentSize;
	}

	public List<Long> getRollupLevels() {
		return rollupLevels;
	}

	public List<Long> getFolderSizes() {
		return folderSizes;
	}

	public Path getCollectionPath() {
		return collectionPath;
	}

	public Path getSegmentPath(long groupingNumber) {
		Function<Long, String> calculateFolderName = (size) -> String.valueOf(groupingNumber / size);
		String[] folderNames= getFolderSizes().stream().map(calculateFolderName).toArray(String[]::new);
		return Paths.get(getCollectionPath().toString(), folderNames);
	}

	public List<Path> getAllPossibleSegmentPaths(BlueKey key) {
		List<Path> paths = new ArrayList<>();
		long groupingNumber = key.getGroupingNumber();
		long i = getSegmentStartGroupingNumber(groupingNumber);
		while (key.overlapsRange(i, i + segmentSize - 1)) {
			Path path = getSegmentPath(i);
			paths.add(path);
			i += segmentSize;
			
			if(key.isActiveTimeKey()) {
				//An active time key would keep going forever but won't ever actually be stored in more than the segment it starts in
				break;
			}
		}
		return paths;
	}

	public long getSegmentStartGroupingNumber(long groupingNumber) {
		return Blutils.roundDownToMultiple(groupingNumber, segmentSize);
	}

	public List<File> getExistingSegmentFiles(Range range) {
		return getExistingSegmentFiles(range.getStart(), range.getEnd());
	}

	public List<File> getExistingSegmentFiles(long minValue, long maxValue) {
		File collectionFolder = getCollectionPath().toFile();
		List<File> foldersAtCurrentLevel = Arrays.asList(collectionFolder);
		for (Long folderSizeThisCurrentLevel: getFolderSizes()) {
			foldersAtCurrentLevel = SegmentPathManager.getSubfoldersInRange(foldersAtCurrentLevel, minValue/folderSizeThisCurrentLevel, maxValue/folderSizeThisCurrentLevel);
		}
		return foldersAtCurrentLevel;
	}

	public static List<File> getSubfoldersInRange(File folder, long minValue, long maxValue) {
		return FileUtils.getSubFolders(folder)
            .stream()
			.filter((f) -> folderNameIsLongInRange(f, minValue, maxValue))
			.collect(Collectors.toList());
	}

	public static List<File> getSubfoldersInRange(List<File> folders, long minValue, long maxValue) {
		List<File> results = new ArrayList<>();
		for (File folder: folders) {
			results.addAll(getSubfoldersInRange(folder, minValue, maxValue));
		}
		return results;
	}
	
	public static boolean folderNameIsLongInRange(File file, long minValue, long maxValue) {
		try {
			long fileNameAsLong = Long.valueOf(file.getName());
			return fileNameAsLong >= minValue && fileNameAsLong <= maxValue;
		} catch(Exception e) {
			return false;
		}
	}
}
