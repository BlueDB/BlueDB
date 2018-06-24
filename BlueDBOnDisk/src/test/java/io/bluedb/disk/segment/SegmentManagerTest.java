package io.bluedb.disk.segment;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
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
		collection = (BlueCollectionImpl) db.getCollection(TestValue.class, "testing");
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
	public void test_fileNameInLongInRange() {
		File file3 = Paths.get("3").toFile();
		File file2to4 = Paths.get("2_4").toFile();
		assertFalse(SegmentManager.fileNameInLongInRange(file2to4, 0, 1));  // should not use folder name convention
		assertFalse(SegmentManager.fileNameInLongInRange(file3, 0, 2));  // below range
		assertTrue(SegmentManager.fileNameInLongInRange(file3, 0, 3));  // at top of range
		assertTrue(SegmentManager.fileNameInLongInRange(file3, 2, 4));  // at middle of range
		assertTrue(SegmentManager.fileNameInLongInRange(file3, 3, 4));  // at bottom of range
		assertFalse(SegmentManager.fileNameInLongInRange(file3, 4, 5));  // above range
		// TODO test at Long.MAX_VALUE, Long.MIN_VALUE
	}

	@Test
	public void test_folderNameInLongInRange() {
		File file2to4 = Paths.get("2_4").toFile();
		assertFalse(SegmentManager.folderNameInLongInRange(file2to4, 0, 1));  // below range
		assertTrue(SegmentManager.folderNameInLongInRange(file2to4, 0, 2));  // touches bottom of range
		assertTrue(SegmentManager.folderNameInLongInRange(file2to4, 2, 3));  // at bottom of range
		assertTrue(SegmentManager.folderNameInLongInRange(file2to4, 3, 4));  // at top of range
		assertTrue(SegmentManager.folderNameInLongInRange(file2to4, 3, 3));  // middle of range
		assertTrue(SegmentManager.folderNameInLongInRange(file2to4, 4, 5));  // touches top of range
		assertFalse(SegmentManager.folderNameInLongInRange(file2to4, 5, 6));  // above range
		// TODO test at Long.MAX_VALUE, Long.MIN_VALUE
	}

	@Test
	public void testGetPath() {
		SegmentManager<TestValue> finder = new SegmentManager<TestValue>(collection);
		BlueKey key = new TimeKey(5, createTime(4, 3, 2, 1));
		List<Path> paths = finder.getAllPossiblePaths(key);
		assertEquals(1, paths.size());
		assertEquals(paths.get(0), finder.getPath(key));
	}

	private long createTime(long level0, long level1, long level2, long level3) {
		return
				level0 * SegmentManager.LEVEL_0 +
				level1 * SegmentManager.LEVEL_1 +
				level2 * SegmentManager.LEVEL_2 +
				level3 * SegmentManager.LEVEL_3;
	}
}
