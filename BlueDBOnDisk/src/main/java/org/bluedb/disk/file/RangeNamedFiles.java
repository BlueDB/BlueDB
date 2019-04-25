package org.bluedb.disk.file;

import java.io.File;
import java.io.FileFilter;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.bluedb.disk.segment.Range;

public class RangeNamedFiles {
	
	protected RangeNamedFiles() {}  // just to get test coverage to 100%

	public static List<File> getOrderedFilesEnclosedInRange(Path segmentPath, Range range) {
		long min = range.getStart();
		long max = range.getEnd();
		File segmentFolder = segmentPath.toFile();
		FileFilter filter = (f) -> isFileNameRangeEnclosed(f, min, max);
		List<File> filesInFolder = FileUtils.getFolderContents(segmentFolder, filter);
		sortByRange(filesInFolder);
		return filesInFolder;
	}

	protected static boolean isFileNameRangeEnclosed(File file, long min, long max ) {
		try {
			String[] splits = file.getName().split("_");
			if (splits.length < 2) {
				return false;
			}
			long start = Long.valueOf(splits[0]);
			long end = Long.valueOf(splits[1]);
			return (end <= max) && (start >= min);
		} catch (Throwable t) {
			return false;
		}
	}

	protected static boolean doesfileNameRangeOverlap(File file, long min, long max ) {
		try {
			String[] splits = file.getName().split("_");
			if (splits.length < 2) {
				return false;
			}
			long start = Long.valueOf(splits[0]);
			long end = Long.valueOf(splits[1]);
			return (start <= max) && (end >= min);
		} catch (Throwable t) {
			return false;
		}
	}

	public static void sortByRange(List<File> files) {
		Comparator<File> comparator = new Comparator<File>() {
			@Override
			public int compare(File o1, File o2) {
				Range r1 = Range.fromUnderscoreDelmimitedString(o1.getName());
				Range r2 = Range.fromUnderscoreDelmimitedString(o2.getName());
				return r1.compareTo(r2);
			}
		};
		Collections.sort(files, comparator);
	}

	public static String getRangeFileName(long groupingValue, long multiple) {
		Range timeRange = Range.forValueAndRangeSize(groupingValue, multiple);
		return timeRange.toUnderscoreDelimitedString();
	}

	public static List<File> getOrderedFilesInRange(Path segmentPath, Range range) {
		long min = range.getStart();
		long max = range.getEnd();
		File segmentFolder = segmentPath.toFile();
		FileFilter filter = (f) -> doesfileNameRangeOverlap(f, min, max);
		List<File> filesInFolder = FileUtils.getFolderContents(segmentFolder, filter);
		sortByRange(filesInFolder);
		return filesInFolder;
	}

}
