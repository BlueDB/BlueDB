package io.bluedb.disk.segment;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.TestValue;

public class SegmentManagerTest extends BlueDbDiskTestBase {

	@Test
	public void test_getRangeFileName() {
		assertEquals("0_1", SegmentManager.getRangeFileName(0, 2));  // test zero
		assertEquals("2_3", SegmentManager.getRangeFileName(2, 2));  // test next doesn't overlap
		assertEquals("4_5", SegmentManager.getRangeFileName(5, 2));  // test greater than a multiple
		assertEquals("0_41", SegmentManager.getRangeFileName(41, 42));  // test equal to a multiple
		assertEquals("42_83", SegmentManager.getRangeFileName(42, 42));  // test equal to a multiple
		assertEquals("42_83", SegmentManager.getRangeFileName(42, 42));  // test equal to a multiple
		assertEquals("-2_-1", SegmentManager.getRangeFileName(-1, 2));  // test zero
		
		String maxLongFileName = SegmentManager.getRangeFileName(Long.MAX_VALUE, 100);
		Range maxLongRange = Range.fromUnderscoreDelmimitedString(maxLongFileName);
		assertTrue(maxLongRange.getEnd() > maxLongRange.getStart());
		assertEquals(Long.MAX_VALUE, maxLongRange.getEnd());

		String minLongFileName = SegmentManager.getRangeFileName(Long.MIN_VALUE, 100);
		Range minLongRange = Range.fromUnderscoreDelmimitedString(minLongFileName);
		assertTrue(minLongRange.getEnd() > minLongRange.getStart());
		assertEquals(Long.MIN_VALUE, minLongRange.getStart());
	}

	@Test
	public void test_getSegmentTimeRange() {
		Range segmentRangeStartingAtZero = SegmentManager.getSegmentTimeRange(0);
		assertEquals(0, segmentRangeStartingAtZero.getStart());
		assertEquals(SegmentManager.getSegmentSize() - 1, segmentRangeStartingAtZero.getEnd());

		Range maxLongRange = SegmentManager.getSegmentTimeRange(Long.MAX_VALUE);
		assertTrue(maxLongRange.getEnd() > maxLongRange.getStart());
		assertEquals(Long.MAX_VALUE, maxLongRange.getEnd());

		Range minLongRange = SegmentManager.getSegmentTimeRange(Long.MIN_VALUE);
		assertTrue(minLongRange.getEnd() > minLongRange.getStart());
		assertEquals(Long.MIN_VALUE, minLongRange.getStart());
	}

	@Test
	public void test_getTimeRange() {
		Range rangeStartingAtZero = SegmentManager.getTimeRange(0, 100);
		assertEquals(0, rangeStartingAtZero.getStart());
		assertEquals(100 - 1, rangeStartingAtZero.getEnd());

		Range rangeStartingAtMinus100 = SegmentManager.getTimeRange(-100, 100);
		assertEquals(-100, rangeStartingAtMinus100.getStart());
		assertEquals(-1, rangeStartingAtMinus100.getEnd());

		Range maxLongRange = SegmentManager.getTimeRange(Long.MAX_VALUE, 100);
		assertTrue(maxLongRange.getEnd() > maxLongRange.getStart());
		assertEquals(Long.MAX_VALUE, maxLongRange.getEnd());

		Range minLongRange = SegmentManager.getTimeRange(Long.MIN_VALUE, 100);
		assertTrue(minLongRange.getEnd() > minLongRange.getStart());
		assertEquals(Long.MIN_VALUE, minLongRange.getStart());
	}

	@Test
	public void test_getSubfoldersInRange() {
		File folder = Paths.get(".", "test_folder").toFile();
		emptyAndDelete(folder);
		folder.mkdir();

		File folder2 = createSubfolder(folder, 2);
		File folder7 = createSubfolder(folder, 7);
		createSegment(folder, 2L, 2L);  // to make sure segment doesn't get included
		
		List<File> empty = Arrays.asList();
		List<File> only2 = Arrays.asList(folder2);
		List<File> both = Arrays.asList(folder2, folder7);
		
		assertEquals(empty, SegmentManager.getSubfoldersInRange(folder, 0L, 1L));  // folder above range
		assertEquals(only2, SegmentManager.getSubfoldersInRange(folder, 0L, 2L));  // folder at top of range
		assertEquals(only2, SegmentManager.getSubfoldersInRange(folder, 0L, 3L));  // folder overlaps top of range
		assertEquals(empty, SegmentManager.getSubfoldersInRange(folder, 5L, 6L));  // folder below range
		assertEquals(both, SegmentManager.getSubfoldersInRange(folder, 0L, 7L));  //works with multiple files

		emptyAndDelete(folder);
	}

	@Test
	public void test_getExistingSegmentFiles_range() {
		File folder = getCollection().getPath().toFile();
		emptyAndDelete(folder);
		folder.mkdir();

		long startTime = 987654321;

		List<Path> singlePathToAdd = getSegmentManager().getAllPossibleSegmentPaths(startTime, startTime);
		assertEquals(1, singlePathToAdd.size());
		assertEquals(0, getSegmentManager().getExistingSegmentFiles(Long.MIN_VALUE, Long.MAX_VALUE).size());
		for (Path path: singlePathToAdd) {
			path.toFile().mkdirs();
		}
		assertEquals(1, getSegmentManager().getExistingSegmentFiles(Long.MIN_VALUE, Long.MAX_VALUE).size());

		List<Path> fivePaths = getSegmentManager().getAllPossibleSegmentPaths(startTime, startTime + SegmentManager.SIZE_SEGMENT * 4);
		assertEquals(5, fivePaths.size());
		for (Path path: fivePaths) {
			path.toFile().mkdirs();
		}
		assertEquals(5, getSegmentManager().getExistingSegmentFiles(Long.MIN_VALUE, Long.MAX_VALUE).size());

		emptyAndDelete(folder);
	}

	@Test
	public void test_getExistingSegmentFiles_key() {
		File folder = getCollection().getPath().toFile();
		emptyAndDelete(folder);
		folder.mkdir();

		long startTime = 987654321;
		TimeKey timeKey = new TimeKey(1, startTime);
		TimeFrameKey timeFrameKey = new TimeFrameKey(2, startTime, startTime + SegmentManager.SIZE_SEGMENT * 4);

		List<Path> singlePathToAdd = getSegmentManager().getAllPossibleSegmentPaths(timeKey);
		assertEquals(1, singlePathToAdd.size());
		assertEquals(0, getSegmentManager().getExistingSegmentFiles(Long.MIN_VALUE, Long.MAX_VALUE).size());
		for (Path path: singlePathToAdd) {
			path.toFile().mkdirs();
		}
		assertEquals(1, getSegmentManager().getExistingSegmentFiles(Long.MIN_VALUE, Long.MAX_VALUE).size());

		List<Path> fivePaths = getSegmentManager().getAllPossibleSegmentPaths(timeFrameKey);
		assertEquals(5, fivePaths.size());
		for (Path path: fivePaths) {
			path.toFile().mkdirs();
		}
		assertEquals(5, getSegmentManager().getExistingSegmentFiles(Long.MIN_VALUE, Long.MAX_VALUE).size());

		emptyAndDelete(folder);
	}

	@Test
	public void test_getAllPossibleSegmentPaths_range() {
		List<Path> paths = getSegmentManager().getAllPossibleSegmentPaths(0, SegmentManager.SIZE_SEGMENT);
		assertEquals(2, paths.size());
		
		Path secondPath = paths.get(1);
		Path parent = secondPath.getParent();
		Path grandparent = parent.getParent();
		Path greatGrandparent = grandparent.getParent();
		
		String fname = String.valueOf( (SegmentManager.SIZE_SEGMENT / SegmentManager.SIZE_SEGMENT) );
		String parentName = String.valueOf( (SegmentManager.SIZE_SEGMENT / SegmentManager.SIZE_FOLDER_BOTTOM) );
		String grandparentName = String.valueOf( (SegmentManager.SIZE_SEGMENT / SegmentManager.SIZE_FOLDER_MIDDLE) );
		String greatGrandparentName = String.valueOf( (SegmentManager.SIZE_SEGMENT / SegmentManager.SIZE_FOLDER_TOP) );
		assertEquals(fname, secondPath.getFileName().toString());
		assertEquals(parentName, parent.getFileName().toString());
		assertEquals(grandparentName, grandparent.getFileName().toString());
		assertEquals(greatGrandparentName, greatGrandparent.getFileName().toString());
	}

	@Test
	public void test_getAllPossibleSegmentPaths_key() {
		BlueKey timeFrameKey = new TimeFrameKey(1, 0, SegmentManager.SIZE_SEGMENT);
		List<Path> timeFramePaths = getSegmentManager().getAllPossibleSegmentPaths(timeFrameKey);
		List<Path> timeFramePaths_range = getSegmentManager().getAllPossibleSegmentPaths(0, SegmentManager.SIZE_SEGMENT);
		assertEquals(timeFramePaths_range, timeFramePaths);
		
		BlueKey timeKey = new TimeKey(1, SegmentManager.SIZE_SEGMENT);
		List<Path> timePaths = getSegmentManager().getAllPossibleSegmentPaths(timeKey);
		List<Path> timePaths_range = getSegmentManager().getAllPossibleSegmentPaths(SegmentManager.SIZE_SEGMENT, SegmentManager.SIZE_SEGMENT);
		assertEquals(timePaths_range, timePaths);
	}

	@Test
	public void test_getAllSegments() {
		BlueKey timeFrameKey = new TimeFrameKey(1, 0, SegmentManager.SIZE_SEGMENT);
		List<Path> timeFramePaths = getSegmentManager().getAllPossibleSegmentPaths(timeFrameKey);
		List<Segment<TestValue>> timeFrameSegments = getSegmentManager().getAllSegments(timeFrameKey);
		List<Path> timeFrameSegmentPaths = timeFrameSegments.stream().map((s) -> s.getPath()).collect(Collectors.toList());

		assertEquals(timeFramePaths, timeFrameSegmentPaths);
		
		BlueKey timeKey = new TimeKey(1, SegmentManager.SIZE_SEGMENT);
		List<Path> timePaths = getSegmentManager().getAllPossibleSegmentPaths(timeKey);
		List<Segment<TestValue>> timeSegments = getSegmentManager().getAllSegments(timeKey);
		List<Path> segmentPaths = timeSegments.stream().map((s) -> s.getPath()).collect(Collectors.toList());
		assertEquals(timePaths, segmentPaths);
	}

	@Test
	public void test_getExistingSegments() {
		emptyAndDelete(getCollection().getPath().toFile());
		long minTime = 0;
		long maxTime = SegmentManager.SIZE_SEGMENT * 2;
		BlueKey timeFrameKey = new TimeFrameKey(1, minTime, maxTime);  // should barely span 3 segments
		TestValue value = new TestValue("Bob", 0);
		try {
			List<Segment<TestValue>> existingSegments = getSegmentManager().getExistingSegments(minTime, maxTime);
			assertEquals(0, existingSegments.size());
			getCollection().insert(timeFrameKey, value);
			existingSegments = getSegmentManager().getExistingSegments(minTime, maxTime);
			assertEquals(3, existingSegments.size());
			List<Segment<TestValue>> existingSegments0to0 = getSegmentManager().getExistingSegments(minTime, minTime);
			assertEquals(1, existingSegments0to0.size());
			List<Segment<TestValue>> existingSegmentsOutsideRange = getSegmentManager().getExistingSegments(maxTime * 2, maxTime * 2);
			assertEquals(0, existingSegmentsOutsideRange.size());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		emptyAndDelete(getCollection().getPath().toFile());
	}

	@Test
	public void test_getFirstSegment() {
		BlueKey timeFrameKey = new TimeFrameKey(1, SegmentManager.SIZE_SEGMENT, SegmentManager.SIZE_SEGMENT * 2);
		List<Segment<TestValue>> timeFrameSegments = getSegmentManager().getAllSegments(timeFrameKey);
		assertEquals(2, timeFrameSegments.size());
		assertEquals(timeFrameSegments.get(0), getSegmentManager().getFirstSegment(timeFrameKey));
	}

	@Test
	public void test_toSegment() {
		BlueKey key = new TimeKey(5, createTime(4, 3, 2, 1));
		Path path = getSegmentManager().getPath(key);
		Segment<TestValue> segment = getSegmentManager().toSegment(path);
		assertEquals(path, segment.getPath());
	}

	@Test
	public void test_getPath_key() {
		BlueKey key = new TimeKey(5, createTime(4, 3, 2, 1));
		List<Path> paths = getSegmentManager().getAllPossibleSegmentPaths(key);
		assertEquals(1, paths.size());
		assertEquals(paths.get(0), getSegmentManager().getPath(key));
	}



	public File createSegment(File parentFolder, long low, long high) {
		String segmentName = String.valueOf(low) + "_" + String.valueOf(high);
		File file = Paths.get(parentFolder.toPath().toString(), segmentName).toFile();
		file.mkdir();
		return file;
	}

	private SegmentManager<TestValue> getSegmentManager() {
		return getCollection().getSegmentManager();
	}

	private long createTime(long level0, long level1, long level2, long level3) {
		return
				level0 * SegmentManager.SIZE_FOLDER_TOP +
				level1 * SegmentManager.SIZE_FOLDER_MIDDLE +
				level2 * SegmentManager.SIZE_FOLDER_BOTTOM +
				level3 * SegmentManager.SIZE_SEGMENT;
	}
}
