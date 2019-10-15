package org.bluedb.disk.segment.path;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;

import org.bluedb.disk.segment.SegmentSizeSetting;

public class SegmentPathManagerTest {
	public static void main(String[] args) {
		SegmentSizeSetting segmentSizeSetting = SegmentSizeSetting.TIME_1_HOUR;
		LocalDateTime dateTime = LocalDateTime.of(LocalDate.of(2018, 12, 31), LocalTime.of(23, 59, 59));
		
		printPathInformationForTime(segmentSizeSetting, dateTime);
	}

	private static void printPathInformationForTime(SegmentSizeSetting segmentSizeSetting, LocalDateTime dateTime) {
		ZonedDateTime zonedDateTime = dateTime.atZone(ZoneId.systemDefault());
		long groupingNumber = zonedDateTime.toInstant().toEpochMilli();
		DateTimeFormatter localizedDateTimeFormatter = DateTimeFormatter.ofLocalizedDateTime(FormatStyle.LONG);
		System.out.println("Grouping number for datetime: [dateTime]" + localizedDateTimeFormatter.format(zonedDateTime) + " [groupingNumber]" + groupingNumber);
		
		SegmentSizeConfiguration segmentSizeConfig = segmentSizeSetting.getConfig();
		System.out.println(segmentSizeSetting + " Folder Sizes: " + segmentSizeConfig.getFolderSizesTopToBottom());
		
		for(long folderSize : segmentSizeConfig.getFolderSizesTopToBottom()) {
			System.out.println(groupingNumber + " / " + folderSize + " = " + Long.toString((long)(groupingNumber / folderSize)));
		}
		
		SegmentPathManager pathManager = new SegmentPathManager(Paths.get(""), segmentSizeConfig);
		Path segmentPath = pathManager.getSegmentPath(groupingNumber);
		System.out.println("Total path: " + segmentPath);
	}
}
