package org.bluedb.disk.segment.path;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.bluedb.disk.Blutils;
import org.bluedb.disk.collection.config.TestDefaultConfigurationService;
import org.bluedb.disk.encryption.EncryptionServiceWrapper;
import org.bluedb.disk.file.ReadOnlyFileManager;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadOnlySegment;
import org.bluedb.disk.segment.SegmentSizeSetting;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;

public class SegmentPathManagerTest {
	public static void main(String[] args) {
		SegmentSizeSetting segmentSizeSetting = SegmentSizeSetting.TIME_1_HOUR;
		
		LocalDateTime dateTime = LocalDateTime.of(LocalDate.of(2018, 12, 31), LocalTime.of(23, 59, 59));
		ZonedDateTime zonedDateTime = dateTime.atZone(ZoneId.systemDefault());
		long groupingNumber = zonedDateTime.toInstant().toEpochMilli();
		DateTimeFormatter localizedDateTimeFormatter = DateTimeFormatter.ofLocalizedDateTime(FormatStyle.LONG);
		System.out.println("Grouping number for datetime: [dateTime]" + localizedDateTimeFormatter.format(zonedDateTime) + " [groupingNumber]" + groupingNumber);
		
//		long groupingNumber = 4096786442l;
		
		printPathInformationForTime(segmentSizeSetting, groupingNumber);
		
//		findAndPrintTargetSegmentInformation(segmentSizeSetting);
	}

	private static void printPathInformationForTime(SegmentSizeSetting segmentSizeSetting, long groupingNumber) {
		SegmentSizeConfiguration segmentSizeConfig = segmentSizeSetting.getConfig();
		System.out.println(segmentSizeSetting + " Folder Sizes: " + segmentSizeConfig.getFolderSizesTopToBottom());
		
		for(long folderSize : segmentSizeConfig.getFolderSizesTopToBottom()) {
			System.out.println(groupingNumber + " / " + folderSize + " = " + Long.toString((long)(groupingNumber / folderSize)));
		}
		
		SegmentPathManager pathManager = new SegmentPathManager(Paths.get(""), segmentSizeConfig);
		Path segmentPath = pathManager.getSegmentPath(groupingNumber);
		System.out.println("Total path: " + segmentPath);
	}
	
	@SuppressWarnings("unused")
	private static void findAndPrintTargetSegmentInformation(SegmentSizeSetting segmentSizeSetting) {
		System.out.println("Segment Size: " + segmentSizeSetting);
		Range targetRange = new Range(4096786432l, 4097835007l);
		System.out.println("Target range: " + targetRange);
		SegmentPathManager pathManager = new SegmentPathManager(Paths.get(""), segmentSizeSetting.getConfig());
		for(ReadOnlySegment<Serializable> segment : findTargetSegments(segmentSizeSetting, pathManager, targetRange)) {
			System.out.println("Segment: " + segment + " size: " + segmentSizeSetting.getSegmentSize() + " range: " + segment.getRange() + " rangeLength: " + segment.getRange().length());
		}
		System.out.println("Complete!");
	}
	
	private static List<ReadOnlySegment<Serializable>> findTargetSegments(SegmentSizeSetting segmentSizeSetting, SegmentPathManager pathManager, Range targetRange) {
		BlueSerializer serializer = new ThreadLocalFstSerializer(new TestDefaultConfigurationService(), new Class[] {});
		EncryptionServiceWrapper encryptionService = new EncryptionServiceWrapper(null);
		ReadOnlyFileManager fileManager = new ReadOnlyFileManager(serializer, encryptionService);
		
		return getAllPossibleSegmentPaths(pathManager, targetRange).stream()
			.map(segmentPath -> (toSegment(pathManager, fileManager, segmentPath)))
			.sorted()
			.collect(Collectors.toList());
	}

	public static List<Path> getAllPossibleSegmentPaths(SegmentPathManager pathManager, Range targetRange) {
		long segmentSize = pathManager.getSegmentSize();
		List<Path> paths = new ArrayList<>();
		long groupingNumber = targetRange.getStart();
		long minTime = Blutils.roundDownToMultiple(groupingNumber, segmentSize);
		long i = minTime;
		while (targetRange.overlaps(new Range(i, i + segmentSize - 1))) {
			Path path = pathManager.getSegmentPath(i);
			paths.add(path);
			i += segmentSize;
		}
		return paths;
	}
	
	private static ReadOnlySegment<Serializable> toSegment(SegmentPathManager pathManager, ReadOnlyFileManager fileManager, Path path) {
		Range range = toRange(pathManager, path);
		return new ReadOnlySegment<Serializable>(path, range, fileManager, pathManager.getRollupLevels());
	}

	public static Range toRange(SegmentPathManager pathManager, Path path) {
		String filename = path.toFile().getName();
		long rangeStart = Long.valueOf(filename) * pathManager.getSegmentSize();
		Range range = new Range(rangeStart, rangeStart + pathManager.getSegmentSize() - 1);
		return range;
	}
}
