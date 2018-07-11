package io.bluedb.disk.segment;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

import io.bluedb.api.BlueDb;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.BlueDbOnDisk;
import io.bluedb.disk.BlueDbOnDiskBuilder;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.collection.BlueCollectionImpl;
import junit.framework.TestCase;

public class SegmentManagerTest extends TestCase {

	BlueDb db;
	BlueCollectionImpl<TestValue> collection;
	SegmentManager<TestValue> segmentManager;
	Path dbPath;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		dbPath = Paths.get("testing_SegmentManagerTest");
		db = new BlueDbOnDiskBuilder().setPath(dbPath).build();
		collection = (BlueCollectionImpl<TestValue>) db.getCollection(TestValue.class, "test_segment_manager");
		segmentManager = new SegmentManager<TestValue>(collection);
	}

	public void tearDown() throws Exception {
		Files.walk(dbPath)
		.sorted(Comparator.reverseOrder())
		.map(Path::toFile)
		.forEach(File::delete);
	}

	@Test
	public void test_getRangeFileName() {
		assertEquals("0_1", SegmentManager.getRangeFileName(0, 2));  // test zero
		assertEquals("2_3", SegmentManager.getRangeFileName(2, 2));  // test next doesn't overlap
		assertEquals("4_5", SegmentManager.getRangeFileName(5, 2));  // test greater than a multiple
		assertEquals("0_41", SegmentManager.getRangeFileName(41, 42));  // test equal to a multiple
		assertEquals("42_83", SegmentManager.getRangeFileName(42, 42));  // test equal to a multiple
		// TODO test at Long.MAX_VALUE, Long.MIN_VALUE
	}

	@Test
	public void test_folderNameRangeContainsRange() {
		File file2to4 = Paths.get("2_4").toFile();
		assertFalse(SegmentManager.folderNameRangeContainsRange(file2to4, 0, 1));  // below range
		assertTrue(SegmentManager.folderNameRangeContainsRange(file2to4, 0, 2));  // touches bottom of range
		assertTrue(SegmentManager.folderNameRangeContainsRange(file2to4, 2, 3));  // at bottom of range
		assertTrue(SegmentManager.folderNameRangeContainsRange(file2to4, 3, 4));  // at top of range
		assertTrue(SegmentManager.folderNameRangeContainsRange(file2to4, 3, 3));  // middle of range
		assertTrue(SegmentManager.folderNameRangeContainsRange(file2to4, 4, 5));  // touches top of range
		assertFalse(SegmentManager.folderNameRangeContainsRange(file2to4, 5, 6));  // above range
		// TODO test at Long.MAX_VALUE, Long.MIN_VALUE
	}

	// TODO test extreme values
	@Test
	public void test_getSegmentFilesInRange() {
		File folder = Paths.get(".", "test_folder").toFile();
		emptyAndDelete(folder);
		folder.mkdir();

		File segmentAt2 = createSegment(folder, 2L, 2L);
		File segmentAt5 = createSegment(folder, 5L, 5L);
		createJunkFile(folder, "5t");  // make sure junk doesn't get included
		createJunkFolder(folder, "7x");  // make sure junk doesn't get included
		createNonsegmentSubfolder(folder, 2, 3);  // make sure non-segments don't get included

		List<File> empty = Arrays.asList();
		List<File> only2 = Arrays.asList(segmentAt2);
		List<File> both = Arrays.asList(segmentAt2, segmentAt5);

		assertEquals(empty, SegmentManager.getSegmentFilesInRange(folder, 0L, 1L));  // above range
		assertEquals(only2, SegmentManager.getSegmentFilesInRange(folder, 0L, 2L));  // at top of range
		assertEquals(only2, SegmentManager.getSegmentFilesInRange(folder, 0L, 3L));  // at middle of range
		assertEquals(only2, SegmentManager.getSegmentFilesInRange(folder, 2L, 3L));  // at bottom of range
		assertEquals(empty, SegmentManager.getSegmentFilesInRange(folder, 3L, 4L));  // below range
		List<File> filesInWideRange = SegmentManager.getSegmentFilesInRange(folder, 0L, 6L);
		Collections.sort(filesInWideRange);
		assertEquals(both, filesInWideRange);  //works with multiple files

		emptyAndDelete(folder);
	}

	// TODO test extreme values
	@Test
	public void test_getNonsegmentSubfoldersInRange() {
		File folder = Paths.get(".", "test_folder").toFile();
		emptyAndDelete(folder);
		folder.mkdir();

		File folder2to4 = createNonsegmentSubfolder(folder, 2, 4);
		File folder7to8 = createNonsegmentSubfolder(folder, 7, 8);
		createSegment(folder, 2L, 2L);  // to make sure segment doesn't get included
		
		List<File> empty = Arrays.asList();
		List<File> only2to4 = Arrays.asList(folder2to4);
		List<File> both = Arrays.asList(folder2to4, folder7to8);
		
		assertEquals(empty, SegmentManager.getNonsegmentSubfoldersInRange(folder, 0L, 1L));  // folder above range
		assertEquals(only2to4, SegmentManager.getNonsegmentSubfoldersInRange(folder, 0L, 2L));  // folder at top of range
		assertEquals(only2to4, SegmentManager.getNonsegmentSubfoldersInRange(folder, 0L, 3L));  // folder overlaps top of range
		assertEquals(only2to4, SegmentManager.getNonsegmentSubfoldersInRange(folder, 3L, 3L));  // range at middle of folder
		assertEquals(only2to4, SegmentManager.getNonsegmentSubfoldersInRange(folder, 3L, 5L));  // folder overlaps bottom of range
		assertEquals(only2to4, SegmentManager.getNonsegmentSubfoldersInRange(folder, 4L, 6L));  // folder at bottom of range
		assertEquals(empty, SegmentManager.getNonsegmentSubfoldersInRange(folder, 5L, 6L));  // folder below range
		assertEquals(both, SegmentManager.getNonsegmentSubfoldersInRange(folder, 0L, 7L));  //works with multiple files

		emptyAndDelete(folder);
	}

	@Test
	public void test_isSegment() {
		File folder = Paths.get(".", "test_folder").toFile();
		emptyAndDelete(folder);
		folder.mkdir();

		File folder7to8 = createNonsegmentSubfolder(folder, 7, 8);
		File segmentAt2 = createSegment(folder, 2L, 2L);
		File junkFile = createJunkFile(folder, "5t");
		File junkFolder = createJunkFolder(folder, "7x");
		
		assertFalse(SegmentManager.isSegment(folder7to8));
		assertTrue(SegmentManager.isSegment(segmentAt2));
		assertFalse(SegmentManager.isSegment(junkFile));
		assertFalse(SegmentManager.isSegment(junkFolder));

		emptyAndDelete(folder);
	}

	// TODO more edge cases
	@Test
	public void test_getExistingSegmentFiles_range() {
		File folder = collection.getPath().toFile();
		emptyAndDelete(folder);
		folder.mkdir();

		long startTime = 987654321;

		List<Path> singlePathToAdd = segmentManager.getAllPossibleSegmentPaths(startTime, startTime);
		assertEquals(1, singlePathToAdd.size());
		assertEquals(0, segmentManager.getExistingSegmentFiles(Long.MIN_VALUE, Long.MAX_VALUE).size());
		for (Path path: singlePathToAdd) {
			path.toFile().mkdirs();
		}
		assertEquals(1, segmentManager.getExistingSegmentFiles(Long.MIN_VALUE, Long.MAX_VALUE).size());

		List<Path> fivePaths = segmentManager.getAllPossibleSegmentPaths(startTime, startTime + SegmentManager.LEVEL_3 * 4);
		assertEquals(5, fivePaths.size());
		for (Path path: fivePaths) {
			path.toFile().mkdirs();
		}
		assertEquals(5, segmentManager.getExistingSegmentFiles(Long.MIN_VALUE, Long.MAX_VALUE).size());

		emptyAndDelete(folder);
	}

	// TODO more edge cases
	@Test
	public void test_getExistingSegmentFiles_key() {
		File folder = collection.getPath().toFile();
		emptyAndDelete(folder);
		folder.mkdir();

		long startTime = 987654321;
		TimeKey timeKey = new TimeKey(1, startTime);
		TimeFrameKey timeFrameKey = new TimeFrameKey(2, startTime, startTime + SegmentManager.LEVEL_3 * 4);

		List<Path> singlePathToAdd = segmentManager.getAllPossibleSegmentPaths(timeKey);
		assertEquals(1, singlePathToAdd.size());
		assertEquals(0, segmentManager.getExistingSegmentFiles(Long.MIN_VALUE, Long.MAX_VALUE).size());
		for (Path path: singlePathToAdd) {
			path.toFile().mkdirs();
		}
		assertEquals(1, segmentManager.getExistingSegmentFiles(Long.MIN_VALUE, Long.MAX_VALUE).size());

		List<Path> fivePaths = segmentManager.getAllPossibleSegmentPaths(timeFrameKey);
		assertEquals(5, fivePaths.size());
		for (Path path: fivePaths) {
			path.toFile().mkdirs();
		}
		assertEquals(5, segmentManager.getExistingSegmentFiles(Long.MIN_VALUE, Long.MAX_VALUE).size());

		emptyAndDelete(folder);
	}

	@Test
	public void test_getAllPossibleSegmentPaths_range() {
		List<Path> paths = segmentManager.getAllPossibleSegmentPaths(0, SegmentManager.LEVEL_3);
		assertEquals(2, paths.size());
		
		Path secondPath = paths.get(1);
		Path parent = secondPath.getParent();
		Path grandparent = parent.getParent();
		Path greatGrandparent = grandparent.getParent();
		
		String fname = SegmentManager.getRangeFileName(SegmentManager.LEVEL_3, SegmentManager.LEVEL_3) + SegmentManager.SEGMENT_SUFFIX;
		String parentName = SegmentManager.getRangeFileName(SegmentManager.LEVEL_3, SegmentManager.LEVEL_2);
		String grandparentName = SegmentManager.getRangeFileName(SegmentManager.LEVEL_3, SegmentManager.LEVEL_1);
		String greatGrandparentName = SegmentManager.getRangeFileName(SegmentManager.LEVEL_3, SegmentManager.LEVEL_0);
		assertEquals(fname, secondPath.getFileName().toString());
		assertEquals(parentName, parent.getFileName().toString());
		assertEquals(grandparentName, grandparent.getFileName().toString());
		assertEquals(greatGrandparentName, greatGrandparent.getFileName().toString());
	}

	@Test
	public void test_getAllPossibleSegmentPaths_key() {
		BlueKey timeFrameKey = new TimeFrameKey(1, 0, SegmentManager.LEVEL_3);
		List<Path> timeFramePaths = segmentManager.getAllPossibleSegmentPaths(timeFrameKey);
		List<Path> timeFramePaths_range = segmentManager.getAllPossibleSegmentPaths(0, SegmentManager.LEVEL_3);
		assertEquals(timeFramePaths_range, timeFramePaths);
		
		BlueKey timeKey = new TimeKey(1, SegmentManager.LEVEL_3);
		List<Path> timePaths = segmentManager.getAllPossibleSegmentPaths(timeKey);
		List<Path> timePaths_range = segmentManager.getAllPossibleSegmentPaths(SegmentManager.LEVEL_3, SegmentManager.LEVEL_3);
		assertEquals(timePaths_range, timePaths);
	}

	@Test
	public void test_getAllSegments() {
		BlueKey timeFrameKey = new TimeFrameKey(1, 0, SegmentManager.LEVEL_3);
		List<Path> timeFramePaths = segmentManager.getAllPossibleSegmentPaths(timeFrameKey);
		List<Segment<TestValue>> timeFrameSegments = segmentManager.getAllSegments(timeFrameKey);
		List<Path> timeFrameSegmentPaths = timeFrameSegments.stream().map((s) -> s.getPath()).collect(Collectors.toList());

		assertEquals(timeFramePaths, timeFrameSegmentPaths);
		
		BlueKey timeKey = new TimeKey(1, SegmentManager.LEVEL_3);
		List<Path> timePaths = segmentManager.getAllPossibleSegmentPaths(timeKey);
		List<Segment<TestValue>> timeSegments = segmentManager.getAllSegments(timeKey);
		List<Path> segmentPaths = timeSegments.stream().map((s) -> s.getPath()).collect(Collectors.toList());
		assertEquals(timePaths, segmentPaths);
	}

	// TODO test extreme values
	@Test
	public void test_getExistingSegments() {
		emptyAndDelete(collection.getPath().toFile());
		long minTime = 0;
		long maxTime = SegmentManager.LEVEL_3 * 2;
		BlueKey timeFrameKey = new TimeFrameKey(1, minTime, maxTime);  // should barely span 3 segments
		TestValue value = new TestValue("Bob", 0);
		try {
			List<Segment<TestValue>> existingSegments = segmentManager.getExistingSegments(minTime, maxTime);
			assertEquals(0, existingSegments.size());
			collection.insert(timeFrameKey, value);
			existingSegments = segmentManager.getExistingSegments(minTime, maxTime);
			assertEquals(3, existingSegments.size());
			List<Segment<TestValue>> existingSegments0to0 = segmentManager.getExistingSegments(minTime, minTime);
			assertEquals(1, existingSegments0to0.size());
			List<Segment<TestValue>> existingSegmentsOutsideRange = segmentManager.getExistingSegments(maxTime * 2, maxTime * 2);
			assertEquals(0, existingSegmentsOutsideRange.size());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		emptyAndDelete(collection.getPath().toFile());
	}

	@Test
	public void test_getFirstSegment() {
		BlueKey timeFrameKey = new TimeFrameKey(1, SegmentManager.LEVEL_3, SegmentManager.LEVEL_3 * 2);
		List<Segment<TestValue>> timeFrameSegments = segmentManager.getAllSegments(timeFrameKey);
		assertEquals(2, timeFrameSegments.size());
		assertEquals(timeFrameSegments.get(0), segmentManager.getFirstSegment(timeFrameKey));
	}

	@Test
	public void test_toSegment() {
		BlueKey key = new TimeKey(5, createTime(4, 3, 2, 1));
		Path path = segmentManager.getPath(key);
		Segment<TestValue> segment = segmentManager.toSegment(path);
		assertEquals(path, segment.getPath());
	}

	@Test
	public void test_getPath_key() {
		BlueKey key = new TimeKey(5, createTime(4, 3, 2, 1));
		List<Path> paths = segmentManager.getAllPossibleSegmentPaths(key);
		assertEquals(1, paths.size());
		assertEquals(paths.get(0), segmentManager.getPath(key));
	}

	private void emptyAndDelete(File folder) {
		if (folder.exists()) {
			for (File f: folder.listFiles()) {
				if (f.isDirectory()) {
					emptyAndDelete(f);
				} else {
					f.delete();
				}
			}
			folder.delete();
		}
	}

	private long createTime(long level0, long level1, long level2, long level3) {
		return
				level0 * SegmentManager.LEVEL_0 +
				level1 * SegmentManager.LEVEL_1 +
				level2 * SegmentManager.LEVEL_2 +
				level3 * SegmentManager.LEVEL_3;
	}

	private File createJunkFolder(File parentFolder, String folderName) {
		File file = Paths.get(parentFolder.toPath().toString(), folderName).toFile();
		file.mkdir();
		return file;
	}

	private File createJunkFile(File parentFolder, String fileName) {
		File file = Paths.get(parentFolder.toPath().toString(), fileName).toFile();
		try {
			file.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return file;
	}

	private File createNonsegmentSubfolder(File parentFolder, long low, long high) {
		String subfolderName = String.valueOf(low) + "_" + String.valueOf(high);
		File file = Paths.get(parentFolder.toPath().toString(), subfolderName).toFile();
		file.mkdir();
		return file;
	}

	private File createSegment(File parentFolder, long low, long high) {
		String segmentName = String.valueOf(low) + "_" + String.valueOf(high) + SegmentManager.SEGMENT_SUFFIX;
		File file = Paths.get(parentFolder.toPath().toString(), segmentName).toFile();
		file.mkdir();
		return file;
	}
}
