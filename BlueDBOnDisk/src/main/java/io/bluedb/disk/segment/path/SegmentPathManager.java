package io.bluedb.disk.segment.path;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.file.FileManager;


public interface SegmentPathManager {

	public Path getSegmentPath(BlueKey key);

	public Path getSegmentPath(long groupingNumber);

	public List<Path> getAllPossibleSegmentPaths(BlueKey key);

	public List<File> getExistingSegmentFiles(long minValue, long maxValue);

	public long getSegmentSize();

	public List<Long> getRollupLevels();

	public static List<File> getSubfoldersInRange(File folder, long minValue, long maxValue) {
		return FileManager.getFolderContents(folder)
            .stream()
			.filter((f) -> f.isDirectory())
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
