package io.bluedb.disk.segment;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.file.FileManager;
import io.bluedb.disk.lock.BlueWriteLock;
import io.bluedb.disk.recovery.PendingRollup;

public class SegmentTest extends BlueDbDiskTestBase {

	@Test
	public void test_getRangeFileName() {
		assertEquals("0_1", Segment.getRangeFileName(0, 2));  // test zero
		assertEquals("2_3", Segment.getRangeFileName(2, 2));  // test next doesn't overlap
		assertEquals("4_5", Segment.getRangeFileName(5, 2));  // test greater than a multiple
		assertEquals("0_41", Segment.getRangeFileName(41, 42));  // test equal to a multiple
		assertEquals("42_83", Segment.getRangeFileName(42, 42));  // test equal to a multiple
		assertEquals("42_83", Segment.getRangeFileName(42, 42));  // test equal to a multiple
		assertEquals("-2_-1", Segment.getRangeFileName(-1, 2));  // test zero
		
		String maxLongFileName = Segment.getRangeFileName(Long.MAX_VALUE, 100);
		Range maxLongRange = Range.fromUnderscoreDelmimitedString(maxLongFileName);
		assertTrue(maxLongRange.getEnd() > maxLongRange.getStart());
		assertEquals(Long.MAX_VALUE, maxLongRange.getEnd());

		String minLongFileName = Segment.getRangeFileName(Long.MIN_VALUE, 100);
		Range minLongRange = Range.fromUnderscoreDelmimitedString(minLongFileName);
		assertTrue(minLongRange.getEnd() > minLongRange.getStart());
		assertEquals(Long.MIN_VALUE, minLongRange.getStart());
	}

	@Test
	public void test_doesfileNameRangeOverlap() {
		File _x_to_1 = Paths.get("1_x").toFile();
		File _1_to_x = Paths.get("1_x").toFile();
		File _1_to_3 = Paths.get("1_3").toFile();
		File _1 = Paths.get("1_").toFile();
		File _1_to_3_in_subfolder = Paths.get("whatever", "1_3").toFile();
		assertFalse(Segment.doesfileNameRangeOverlap(_1, 0, 10));
		assertFalse(Segment.doesfileNameRangeOverlap(_x_to_1, 0, 10));
		assertFalse(Segment.doesfileNameRangeOverlap(_1_to_x, 0, 10));
		assertTrue(Segment.doesfileNameRangeOverlap(_1_to_3, 0, 10));
		assertTrue(Segment.doesfileNameRangeOverlap(_1_to_3_in_subfolder, 0, 10));
		assertFalse(Segment.doesfileNameRangeOverlap(_1_to_3, 0, 0));  // above range
		assertTrue(Segment.doesfileNameRangeOverlap(_1_to_3, 0, 1));  // top of range
		assertTrue(Segment.doesfileNameRangeOverlap(_1_to_3, 2, 2));  // point
		assertTrue(Segment.doesfileNameRangeOverlap(_1_to_3, 0, 5));  // middle of range
		assertTrue(Segment.doesfileNameRangeOverlap(_1_to_3, 3, 4));  // bottom of range
		assertFalse(Segment.doesfileNameRangeOverlap(_1_to_3, 4, 5));  // below range
	}

	@Test
	public void test_contains() {
		Segment<TestValue> segment = getSegment();
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key2At1 = createKey(2, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		TestValue value3 = createValue("Chuck");
		try {
			assertFalse(segment.contains(key1At1));
			segment.insert(key1At1, value1);
			assertTrue(segment.contains(key1At1));
			assertFalse(segment.contains(key2At1));
			assertFalse(segment.contains(key3At3));
			segment.insert(key2At1, value2);
			assertTrue(segment.contains(key1At1));
			assertTrue(segment.contains(key2At1));
			assertFalse(segment.contains(key3At3));
			segment.insert(key3At3, value3);
			assertTrue(segment.contains(key1At1));
			assertTrue(segment.contains(key2At1));
			assertTrue(segment.contains(key3At3));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_insert() {
		Segment<TestValue> segment = getSegment();
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key2At1 = createKey(2, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		TestValue value3 = createValue("Chuck");
		try {
			assertFalse(segment.contains(key1At1));
			assertFalse(segment.contains(key2At1));
			assertFalse(segment.contains(key3At3));
			assertEquals(null, segment.get(key1At1));
			assertEquals(null, segment.get(key2At1));
			assertEquals(null, segment.get(key3At3));

			segment.insert(key3At3, value3);
			assertFalse(segment.contains(key1At1));
			assertFalse(segment.contains(key2At1));
			assertTrue(segment.contains(key3At3));
			assertEquals(null, segment.get(key1At1));
			assertEquals(null, segment.get(key2At1));
			assertEquals(value3, segment.get(key3At3));

			segment.insert(key2At1, value2);
			assertFalse(segment.contains(key1At1));
			assertTrue(segment.contains(key2At1));
			assertTrue(segment.contains(key3At3));
			assertEquals(null, segment.get(key1At1));
			assertEquals(value2, segment.get(key2At1));
			assertEquals(value3, segment.get(key3At3));

			segment.insert(key1At1, value1);
			assertTrue(segment.contains(key1At1));
			assertTrue(segment.contains(key2At1));
			assertTrue(segment.contains(key3At3));
			assertEquals(value1, segment.get(key1At1));
			assertEquals(value2, segment.get(key2At1));
			assertEquals(value3, segment.get(key3At3));

			// make sure insert works after rollup
			segment.rollup(getTimeCollection().getSegmentManager().getSegmentRange(0));
			BlueKey key4At2 = createKey(4, 2);
			TestValue value4 = createValue("Dan");
			segment.insert(key4At2, value4);
			assertTrue(segment.contains(key1At1));
			assertTrue(segment.contains(key2At1));
			assertTrue(segment.contains(key3At3));
			assertTrue(segment.contains(key4At2));

		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void testDelete() {
		Segment<TestValue> segment = getSegment();
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key2At1 = createKey(2, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		TestValue value3 = createValue("Chuck");
		try {
			segment.insert(key1At1, value1);
			segment.insert(key2At1, value2);
			segment.insert(key3At3, value3);
			assertTrue(segment.contains(key1At1));
			assertTrue(segment.contains(key2At1));
			assertTrue(segment.contains(key3At3));
			assertEquals(value1, segment.get(key1At1));
			assertEquals(value2, segment.get(key2At1));
			assertEquals(value3, segment.get(key3At3));
			segment.delete(key1At1);
			assertFalse(segment.contains(key1At1));
			assertTrue(segment.contains(key2At1));
			assertTrue(segment.contains(key3At3));
			assertEquals(null, segment.get(key1At1));
			assertEquals(value2, segment.get(key2At1));
			assertEquals(value3, segment.get(key3At3));
			segment.delete(key2At1);
			assertFalse(segment.contains(key1At1));
			assertFalse(segment.contains(key2At1));
			assertTrue(segment.contains(key3At3));
			assertEquals(null, segment.get(key1At1));
			assertEquals(null, segment.get(key2At1));
			assertEquals(value3, segment.get(key3At3));
			segment.delete(key3At3);
			assertFalse(segment.contains(key1At1));
			assertFalse(segment.contains(key2At1));
			assertFalse(segment.contains(key3At3));
			assertEquals(null, segment.get(key1At1));
			assertEquals(null, segment.get(key2At1));
			assertEquals(null, segment.get(key3At3));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void testGet() {
		Segment<TestValue> segment = getSegment();
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key2At1 = createKey(2, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		TestValue value3 = createValue("Chuck");
		try {
			assertEquals(null, segment.get(key1At1));
			assertEquals(null, segment.get(key2At1));
			assertEquals(null, segment.get(key3At3));

			segment.insert(key3At3, value3);
			assertEquals(null, segment.get(key1At1));
			assertEquals(null, segment.get(key2At1));
			assertEquals(value3, segment.get(key3At3));

			segment.insert(key2At1, value2);
			assertEquals(null, segment.get(key1At1));
			assertEquals(value2, segment.get(key2At1));
			assertEquals(value3, segment.get(key3At3));

			segment.insert(key1At1, value1);
			assertEquals(value1, segment.get(key1At1));
			assertEquals(value2, segment.get(key2At1));
			assertEquals(value3, segment.get(key3At3));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void testGetAll() {
		Segment<TestValue> segment = getSegment();
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key2At1 = createKey(2, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		TestValue value3 = createValue("Chuck");
		List<TestValue> values;
		try {
			values = getAll(segment);
			assertEquals(0, values.size());

			segment.insert(key1At1, value1);
			values = getAll(segment);
			assertEquals(1, values.size());
			assertTrue(values.contains(value1));

			segment.insert(key2At1, value2);
			values = getAll(segment);
			assertEquals(2, values.size());
			assertTrue(values.contains(value1));
			assertTrue(values.contains(value2));

			segment.insert(key3At3, value3);
			values = getAll(segment);
			assertEquals(3, values.size());
			assertTrue(values.contains(value1));
			assertTrue(values.contains(value2));
			assertTrue(values.contains(value3));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_rollup() {
		Segment<TestValue> segment = getSegment();
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value3 = createValue("Chuck");
		List<TestValue> values;
		try {
			values = getAll(segment);
			assertEquals(0, values.size());

			segment.insert(key1At1, value1);
			segment.insert(key3At3, value3);
			values = getAll(segment);
			assertEquals(2, values.size());
			File[] directoryContents = segment.getPath().toFile().listFiles();
			assertEquals(2, directoryContents.length);

			Range invalidRollupTimeRange = new Range(0, 3);
			try {
				segment.rollup(invalidRollupTimeRange);
				fail();  // rollups must be 
			} catch (BlueDbException e) {}

			Range validRollupTimeRange = new Range(0, getTimeCollection().getSegmentManager().getSegmentSize() - 1);
			segment.rollup(validRollupTimeRange);
			values = getAll(segment);
			assertEquals(2, values.size());
			directoryContents = segment.getPath().toFile().listFiles();
			assertEquals(1, directoryContents.length);

		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_getOrderedFilesInRange() {
		File _12_13 = Paths.get(getPath().toString(), "12_13").toFile();
		File _12_15 = Paths.get(getPath().toString(), "12_15").toFile();
		File _2_3 = Paths.get(getPath().toString(), "2_3").toFile();
		File _100_101 = Paths.get(getPath().toString(), "100_101").toFile();
		List<File> expected = Arrays.asList(_2_3, _12_13, _12_15);

		try {
			FileManager.ensureFileExists(_12_13.toPath());
			FileManager.ensureFileExists(_12_15.toPath());
			FileManager.ensureFileExists(_2_3.toPath());
			FileManager.ensureFileExists(_100_101.toPath());
			Range timeRange = new Range(0, 20);
			assertEquals(expected, Segment.getOrderedFilesInRange(getPath(), timeRange));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_sortByRange() {
		File _12_13 = Paths.get(getPath().toString(), "12_13").toFile();
		File _12_15 = Paths.get(getPath().toString(), "12_15").toFile();
		File _2_3 = Paths.get(getPath().toString(), "2_3").toFile();
		List<File> unsorted = Arrays.asList(_12_15, _2_3, _12_13);
		List<File> sorted = Arrays.asList(_2_3, _12_13, _12_15);

		assertFalse(unsorted.equals(sorted));
		Segment.sortByRange(unsorted);
		assertTrue(unsorted.equals(sorted));
	}

	@Test
	public void testToString() {
		Segment<TestValue> segment = getSegment();
		assertTrue(segment.toString().contains(segment.getPath().toString()));
		assertTrue(segment.toString().contains(segment.getClass().getSimpleName()));
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void test_equals() {
		Segment<TestValue> segment1 = getSegment(1);
		Segment<TestValue> segment1copy = getSegment(1);
		Segment<TestValue> segmentMax = getSegment(Long.MAX_VALUE);
		Segment<TestValue> segmentNullPath = Segment.getTestSegment();
		Segment<TestValue> segmentNullPathCopy = Segment.getTestSegment();
		assertEquals(segment1, segment1copy);
		assertFalse(segment1.equals(segmentMax));
		assertFalse(segment1.equals(null));
		assertFalse(segmentNullPath.equals(segment1));
		assertFalse(segment1.equals(segmentNullPath));
		assertFalse(segment1.equals("this is a String"));
		assertTrue(segmentNullPath.equals(segmentNullPathCopy));
	}

	@Test
	public void test_hashCode() {
		Segment<TestValue> segment1 = getSegment(1);
		Segment<TestValue> segment1copy = getSegment(1);
		Segment<TestValue> segmentMax = getSegment(Long.MAX_VALUE);
		Segment<TestValue> segmentNullPath = Segment.getTestSegment();
		assertEquals(segment1.hashCode(), segment1copy.hashCode());
		assertTrue(segment1.hashCode() != segmentMax.hashCode());
		assertTrue(segment1.hashCode() != segmentNullPath.hashCode());
	}

	@Test
	public void test_recover_pendingRollup_crash_before_rollup_starts() {
		BlueKey key1 = createKey(1, 1);
		BlueKey key2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		try {
			getTimeCollection().insert(key1, value1);
			getTimeCollection().insert(key2, value2);
			Range rollupRange = getTimeCollection().getSegmentManager().getSegmentRange(0);
			PendingRollup<TestValue> pendingRollup = new PendingRollup<TestValue>(rollupRange);
			getRecoveryManager().saveChange(pendingRollup);

			Segment<TestValue> segment = getTimeCollection().getSegmentManager().getFirstSegment(key1);

			getRecoveryManager().recover();
			List<File> remainingFiles = segment.getOrderedFilesInRange(rollupRange);	
			assertEquals(1, remainingFiles.size());
			List<TestValue> values = getTimeCollection().query().getList();
			assertEquals(2, values.size());
			assertTrue(values.contains(value1));
			assertTrue(values.contains(value2));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_recover_pendingRollup_crash_before_move_tmp() {
		BlueKey key1 = createKey(1, 1);
		BlueKey key2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		try {
			getTimeCollection().insert(key1, value1);
			getTimeCollection().insert(key2, value2);
			Range rollupRange = getTimeCollection().getSegmentManager().getSegmentRange(0);
			PendingRollup<TestValue> pendingRollup = new PendingRollup<TestValue>(rollupRange);
			getRecoveryManager().saveChange(pendingRollup);

			Segment<TestValue> segment = getTimeCollection().getSegmentManager().getFirstSegment(key1);
			List<File> filesToRollup = segment.getOrderedFilesInRange(rollupRange);	
			assertEquals(2, filesToRollup.size());
			Path rolledUpPath = Paths.get(segment.getPath().toString(), rollupRange.toUnderscoreDelimitedString());
			Path tmpPath = FileManager.createTempFilePath(rolledUpPath);
			segment.copy(tmpPath, filesToRollup);

			
			File[] filesExistingAfterCopy = tmpPath.toFile().getParentFile().listFiles(); //segment.getOrderedFilesInRange(rollupRange);	
			assertEquals(3, filesExistingAfterCopy.length);

			getRecoveryManager().recover();
			List<File> remainingFiles = segment.getOrderedFilesInRange(rollupRange);	
			assertEquals(1, remainingFiles.size());
			List<TestValue> values = getTimeCollection().query().getList();
			assertEquals(2, values.size());
			assertTrue(values.contains(value1));
			assertTrue(values.contains(value2));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_recover_pendingRollup_crash_before_deletes_finished() {
		BlueKey key1 = createKey(1, 1);
		BlueKey key2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		try {
			getTimeCollection().insert(key1, value1);
			getTimeCollection().insert(key2, value2);
			Range rollupRange = getTimeCollection().getSegmentManager().getSegmentRange(0);
			PendingRollup<TestValue> pendingRollup = new PendingRollup<TestValue>(rollupRange);
			getRecoveryManager().saveChange(pendingRollup);

			Segment<TestValue> segment = getTimeCollection().getSegmentManager().getFirstSegment(key1);
			List<File> filesToRollup = segment.getOrderedFilesInRange(rollupRange);	
			assertEquals(2, filesToRollup.size());
			Path rolledUpPath = Paths.get(segment.getPath().toString(), rollupRange.toUnderscoreDelimitedString());
			Path tmpPath = FileManager.createTempFilePath(rolledUpPath);
			segment.copy(tmpPath, filesToRollup);

			
			File[] filesExistingAfterCopy = tmpPath.toFile().getParentFile().listFiles(); //segment.getOrderedFilesInRange(rollupRange);	
			assertEquals(3, filesExistingAfterCopy.length);

			try (BlueWriteLock<Path> targetFileLock = getLockManager().acquireWriteLock(rolledUpPath)) {
				FileManager.moveFile(tmpPath, targetFileLock);
			}
			File file = filesToRollup.get(0);
			try (BlueWriteLock<Path> writeLock = getLockManager().acquireWriteLock(file.toPath())) {
				FileManager.deleteFile(writeLock);
			}

			getRecoveryManager().recover();
			List<File> remainingFiles = segment.getOrderedFilesInRange(rollupRange);	
			assertEquals(1, remainingFiles.size());
			List<TestValue> values = getTimeCollection().query().getList();
			assertEquals(2, values.size());
			assertTrue(values.contains(value1));
			assertTrue(values.contains(value2));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_recover_pendingRollup_crash_after_completed() {
		BlueKey key1 = createKey(1, 1);
		BlueKey key2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		try {
			getTimeCollection().insert(key1, value1);
			getTimeCollection().insert(key2, value2);
			Range rollupRange = getTimeCollection().getSegmentManager().getSegmentRange(0);
			PendingRollup<TestValue> pendingRollup = new PendingRollup<TestValue>(rollupRange);
			getRecoveryManager().saveChange(pendingRollup);

			Segment<TestValue> segment = getTimeCollection().getSegmentManager().getFirstSegment(key1);
			List<File> filesToRollup = segment.getOrderedFilesInRange(rollupRange);	
			assertEquals(2, filesToRollup.size());
			Path rolledUpPath = Paths.get(segment.getPath().toString(), rollupRange.toUnderscoreDelimitedString());
			Path tmpPath = FileManager.createTempFilePath(rolledUpPath);
			segment.copy(tmpPath, filesToRollup);

			
			File[] filesExistingAfterCopy = tmpPath.toFile().getParentFile().listFiles(); //segment.getOrderedFilesInRange(rollupRange);	
			assertEquals(3, filesExistingAfterCopy.length);

			try (BlueWriteLock<Path> targetFileLock = getLockManager().acquireWriteLock(rolledUpPath)) {
				FileManager.moveFile(tmpPath, targetFileLock);
			}
			for (File file: filesToRollup) {
				try (BlueWriteLock<Path> writeLock = getLockManager().acquireWriteLock(file.toPath())) {
					FileManager.deleteFile(writeLock);
				}
			}

			getRecoveryManager().recover();
			List<File> remainingFiles = segment.getOrderedFilesInRange(rollupRange);	
			assertEquals(1, remainingFiles.size());
			List<TestValue> values = getTimeCollection().query().getList();
			assertEquals(2, values.size());
			assertTrue(values.contains(value1));
			assertTrue(values.contains(value2));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}
}
