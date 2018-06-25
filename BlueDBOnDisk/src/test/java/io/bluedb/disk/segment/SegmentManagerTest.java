package io.bluedb.disk.segment;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import io.bluedb.api.BlueDb;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.BlueDbOnDiskBuilder;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.collection.BlueCollectionImpl;
import junit.framework.TestCase;

public class SegmentManagerTest extends TestCase {

	BlueDb db;
	BlueCollectionImpl<TestValue> collection;
	
	@SuppressWarnings("unchecked")
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		db = new BlueDbOnDiskBuilder().build();
		collection = (BlueCollectionImpl) db.getCollection(TestValue.class, "test_segment_manager");
	}

	@Test
	public void test_roundDownToMultiple() {
		assertEquals(0, SegmentManager.roundDownToMultiple(0, 2));  // test zero
		assertEquals(4, SegmentManager.roundDownToMultiple(5, 2));  // test greater than a multiple
		assertEquals(0, SegmentManager.roundDownToMultiple(41, 42));  // test equal to a multiple
		assertEquals(42, SegmentManager.roundDownToMultiple(42, 42));  // test equal to a multiple
		// TODO test at Long.MAX_VALUE, Long.MIN_VALUE
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
	public void test_isFileNameALongInRange() {
		File file3 = Paths.get("3").toFile();
		File file2to4 = Paths.get("2_4").toFile();
		assertFalse(SegmentManager.isFileNameALongInRange(file2to4, 0, 1));  // should not use folder name convention
		assertFalse(SegmentManager.isFileNameALongInRange(file3, 0, 2));  // below range
		assertTrue(SegmentManager.isFileNameALongInRange(file3, 0, 3));  // at top of range
		assertTrue(SegmentManager.isFileNameALongInRange(file3, 2, 4));  // at middle of range
		assertTrue(SegmentManager.isFileNameALongInRange(file3, 3, 4));  // at bottom of range
		assertFalse(SegmentManager.isFileNameALongInRange(file3, 4, 5));  // above range
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
		cleanupTestFolder(folder);
		folder.mkdir();
		File segmentAt2 = Paths.get(folder.toString(), "2").toFile();
		File segmentAt5 = Paths.get(folder.toString(), "5").toFile();
		File junk = Paths.get(folder.toString(), "5T").toFile();
		File folderInRange = Paths.get(folder.toString(), "2_3").toFile();
		folderInRange.mkdir();
		List<File> empty = Arrays.asList();
		List<File> only2 = Arrays.asList(segmentAt2);
		List<File> both = Arrays.asList(segmentAt2, segmentAt5);
		try {
			segmentAt2.createNewFile();
			segmentAt5.createNewFile();
			junk.createNewFile();
			assertEquals(empty, SegmentManager.getSegmentFilesInRange(folder, 0L, 1L));  // above range
			assertEquals(only2, SegmentManager.getSegmentFilesInRange(folder, 0L, 2L));  // at top of range
			assertEquals(only2, SegmentManager.getSegmentFilesInRange(folder, 0L, 3L));  // at middle of range
			assertEquals(only2, SegmentManager.getSegmentFilesInRange(folder, 2L, 3L));  // at bottom of range
			assertEquals(empty, SegmentManager.getSegmentFilesInRange(folder, 3L, 4L));  // below range
			assertEquals(both, SegmentManager.getSegmentFilesInRange(folder, 0L, 6L));  //works with multiple files
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		cleanupTestFolder(folder);
	}

	// TODO test extreme values
	@Test
	public void test_getSubfoldersInRange() {
		File folder = Paths.get(".", "test_folder").toFile();
		cleanupTestFolder(folder);
		folder.mkdir();
		File folder2to4 = Paths.get(folder.toString(), "2_4").toFile();
		File folder7to8 = Paths.get(folder.toString(), "7_8").toFile();
		folder2to4.mkdir();
		folder7to8.mkdir();
		File segmentAt2 = Paths.get(folder.toString(), "2").toFile();
		File nonsenseFile = Paths.get(folder.toString(), "2_4").toFile();
		List<File> empty = Arrays.asList();
		List<File> only2to4 = Arrays.asList(folder2to4);
		List<File> both = Arrays.asList(folder2to4, folder7to8);
		try {
			segmentAt2.createNewFile();
			nonsenseFile.createNewFile();
			assertEquals(empty, SegmentManager.getSubfoldersInRange(folder, 0L, 1L));  // folder above range
			assertEquals(only2to4, SegmentManager.getSubfoldersInRange(folder, 0L, 2L));  // folder at top of range
			assertEquals(only2to4, SegmentManager.getSubfoldersInRange(folder, 0L, 3L));  // folder overlaps top of range
			assertEquals(only2to4, SegmentManager.getSubfoldersInRange(folder, 3L, 3L));  // range at middle of folder
			assertEquals(only2to4, SegmentManager.getSubfoldersInRange(folder, 3L, 5L));  // folder overlaps bottom of range
			assertEquals(only2to4, SegmentManager.getSubfoldersInRange(folder, 4L, 6L));  // folder at bottom of range
			assertEquals(empty, SegmentManager.getSubfoldersInRange(folder, 5L, 6L));  // folder below range
			assertEquals(both, SegmentManager.getSubfoldersInRange(folder, 0L, 7L));  //works with multiple files
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		cleanupTestFolder(folder);
	}

	@Test
	public void test_getPath() {
		SegmentManager<TestValue> finder = new SegmentManager<TestValue>(collection);
		BlueKey key = new TimeKey(5, createTime(4, 3, 2, 1));
		List<Path> paths = finder.getAllPossiblePaths(key);
		assertEquals(1, paths.size());
		assertEquals(paths.get(0), finder.getPath(key));
	}

	private void cleanupTestFolder(File folder) {
		if (folder.exists()) {
			for (File f: folder.listFiles()) {
				f.delete();
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
}
