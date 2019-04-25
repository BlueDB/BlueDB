package org.bluedb.disk.segment;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.file.FileUtils;
import org.bluedb.disk.lock.BlueWriteLock;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.recovery.PendingRollup;
import org.bluedb.disk.segment.rollup.RollupTarget;

public class SegmentTest extends BlueDbDiskTestBase {

	@Test
	public void test_contains() throws Exception {
		Segment<TestValue> segment = getSegment();
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key2At1 = createKey(2, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		TestValue value3 = createValue("Chuck");

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
	}

	@Test
	public void test_insert() throws Exception {
		Segment<TestValue> segment = getSegment();
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key2At1 = createKey(2, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		TestValue value3 = createValue("Chuck");

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
	}

	@Test
	public void testDelete() throws Exception {
		Segment<TestValue> segment = getSegment();
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key2At1 = createKey(2, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		TestValue value3 = createValue("Chuck");

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
	}


	@Test
	public void test_applyChanges() throws Exception {
		Segment<TestValue> segment = getSegment();
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key2At1 = createKey(2, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		TestValue value3 = createValue("Chuck");

		assertFalse(segment.contains(key1At1));
		assertFalse(segment.contains(key2At1));
		assertFalse(segment.contains(key3At3));
		assertEquals(null, segment.get(key1At1));
		assertEquals(null, segment.get(key2At1));
		assertEquals(null, segment.get(key3At3));

		IndividualChange<TestValue> insert1At1 = IndividualChange.createInsertChange(key1At1, value1);
		IndividualChange<TestValue> insert2At1 = IndividualChange.createInsertChange(key2At1, value2);
		IndividualChange<TestValue> insert3At3 = IndividualChange.createInsertChange(key3At3, value3);
		List<IndividualChange<TestValue>> changes = Arrays.asList(insert1At1, insert2At1, insert3At3);
		LinkedList<IndividualChange<TestValue>> changesLinkedList = new LinkedList<>(changes);
		segment.applyChanges(changesLinkedList);

		assertTrue(segment.contains(key1At1));
		assertTrue(segment.contains(key2At1));
		assertTrue(segment.contains(key3At3));
		assertEquals(value1, segment.get(key1At1));
		assertEquals(value2, segment.get(key2At1));
		assertEquals(value3, segment.get(key3At3));
	}

	@Test
	public void test_applyChanges_preBatchRollup() throws Exception {
		Segment<TestValue> segment = getSegment();
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key2At2 = createKey(2, 2);
		BlueKey keySegmentEnd = createKey(3, segment.getRange().getEnd());
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		TestValue value3 = createValue("Charlie");

		List<TestValue> listEmpty = Arrays.asList();
		List<TestValue> list1 = Arrays.asList(value1);
		List<TestValue> list1and2 = Arrays.asList(value1, value2);
		List<TestValue> list1and2and3 = Arrays.asList(value1, value2, value3);

		assertEquals(listEmpty, getSegmentContents(segment));
		assertEquals(0, Segment.getAllFileRangesInOrder(segment.getPath()).size());

		segment.insert(key1At1, value1);
		assertEquals(list1, getSegmentContents(segment));
		assertEquals(1, Segment.getAllFileRangesInOrder(segment.getPath()).size());

		IndividualChange<TestValue> insert2At1 = IndividualChange.createInsertChange(key2At2, value2);
		List<IndividualChange<TestValue>> changes = Arrays.asList(insert2At1);
		LinkedList<IndividualChange<TestValue>> changesLinkedList = new LinkedList<>(changes);
		segment.applyChanges(changesLinkedList);
		assertEquals(list1and2, getSegmentContents(segment));
		assertEquals(1, Segment.getAllFileRangesInOrder(segment.getPath()).size());

		IndividualChange<TestValue> inserinset3AtSegmentEnd = IndividualChange.createInsertChange(keySegmentEnd, value3);
		changes = Arrays.asList(inserinset3AtSegmentEnd);
		changesLinkedList = new LinkedList<>(changes);
		segment.applyChanges(changesLinkedList);
		assertEquals(list1and2and3, getSegmentContents(segment));
		assertEquals(2, Segment.getAllFileRangesInOrder(segment.getPath()).size());
	}

	public static <T extends Serializable> List<T> getSegmentContents(Segment<T> segment) {
		List<T> results = new ArrayList<>();
		segment.getIterator(Long.MIN_VALUE, Long.MAX_VALUE).forEachRemaining((entity) -> {
			results.add(entity.getValue());
		});
		return results;
	}

	@Test
	public void testGet() throws Exception {
		Segment<TestValue> segment = getSegment();
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key2At1 = createKey(2, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		TestValue value3 = createValue("Chuck");

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
	}

	@Test
	public void testRange() throws Exception {
		long segmentSize = getTimeCollection().getSegmentManager().getSegmentSize();
		Segment<TestValue> firstSegment = getSegment(0);
		Segment<TestValue> secondSegment = getSegment(segmentSize);
		Range expectedFirstRange = new Range(0, segmentSize - 1);
		assertEquals(expectedFirstRange, firstSegment.getRange());
		assertEquals(firstSegment.getRange().getEnd() + 1, secondSegment.getRange().getStart());
	}

	@Test
	public void testGetAll() throws Exception {
		Segment<TestValue> segment = getSegment();
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key2At1 = createKey(2, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		TestValue value3 = createValue("Chuck");
		List<TestValue> values;

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
	}

	@Test
	public void test_rollup() throws Exception {
		Segment<TestValue> segment = getSegment();
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value3 = createValue("Chuck");
		List<TestValue> values;

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
	}


	@Test
	public void test_rollup_out_of_order() throws Exception {
		Segment<TestValue> segment = getSegment();
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value3 = createValue("Chuck");
		segment.insert(key1At1, value1);
		segment.insert(key3At3, value3);

		File[] directoryContentsBeforeRollups = segment.getPath().toFile().listFiles();
		assertEquals(2, directoryContentsBeforeRollups.length);

		Range topRollupRange = new Range(0, 3_600_000 - 1);
		segment.rollup(topRollupRange);
		File[] directoryContentsAfterTopRollup = segment.getPath().toFile().listFiles();
		assertEquals(1, directoryContentsAfterTopRollup.length);

		Range midRollupRange = new Range(0, 6_000 - 1);
		segment.rollup(midRollupRange);
		File[] directoryContentsAfterLaterMidRollup = segment.getPath().toFile().listFiles();
		assertEquals(1, directoryContentsAfterLaterMidRollup.length);
		assertEquals(directoryContentsAfterTopRollup[0].getName(), directoryContentsAfterLaterMidRollup[0].getName());
	}

	@Test
	public void test_getAllFileRangesInOrder() throws Exception {
		File _12_13 = Paths.get(getPath().toString(), "12_13").toFile();
		File _12_15 = Paths.get(getPath().toString(), "12_15").toFile();
		File _2_3 = Paths.get(getPath().toString(), "2_3").toFile();
		File _100_101 = Paths.get(getPath().toString(), "100_101").toFile();

		FileUtils.ensureFileExists(_12_13.toPath());
		FileUtils.ensureFileExists(_12_15.toPath());
		FileUtils.ensureFileExists(_2_3.toPath());
		FileUtils.ensureFileExists(_100_101.toPath());

		Range range_2_3 = Range.fromUnderscoreDelmimitedString(_2_3.getName());
		Range range_12_13 = Range.fromUnderscoreDelmimitedString(_12_13.getName());
		Range range_12_15 = Range.fromUnderscoreDelmimitedString(_12_15.getName());
		Range range_100_101 = Range.fromUnderscoreDelmimitedString(_100_101.getName());
		List<Range> expected = Arrays.asList(range_2_3, range_12_13, range_12_15, range_100_101);
		
		assertEquals(expected, Segment.getAllFileRangesInOrder(getPath()));
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
	public void test_recover_pendingRollup_crash_before_rollup_starts() throws Exception {
		BlueKey key1 = createKey(1, 1);
		BlueKey key2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");

		getTimeCollection().insert(key1, value1);
		getTimeCollection().insert(key2, value2);
		Range rollupRange = getTimeCollection().getSegmentManager().getSegmentRange(0);
		RollupTarget rollupTarget = new RollupTarget(0, rollupRange);
		PendingRollup<TestValue> pendingRollup = new PendingRollup<TestValue>(rollupTarget);
		getRecoveryManager().saveChange(pendingRollup);

		Segment<TestValue> segment = getTimeCollection().getSegmentManager().getFirstSegment(key1);

		getRecoveryManager().recover();
		List<File> remainingFiles = segment.getOrderedFilesInRange(rollupRange);
		assertEquals(1, remainingFiles.size());
		List<TestValue> values = getTimeCollection().query().getList();
		assertEquals(2, values.size());
		assertTrue(values.contains(value1));
		assertTrue(values.contains(value2));
	}

	@Test
	public void test_recover_pendingRollup_crash_before_move_tmp() throws Exception {
		BlueKey key1 = createKey(1, 1);
		BlueKey key2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");

		getTimeCollection().insert(key1, value1);
		getTimeCollection().insert(key2, value2);
		Range rollupRange = getTimeCollection().getSegmentManager().getSegmentRange(0);
		RollupTarget rollupTarget = new RollupTarget(0, rollupRange);
		PendingRollup<TestValue> pendingRollup = new PendingRollup<TestValue>(rollupTarget);
		getRecoveryManager().saveChange(pendingRollup);

		Segment<TestValue> segment = getTimeCollection().getSegmentManager().getFirstSegment(key1);
		List<File> filesToRollup = segment.getOrderedFilesInRange(rollupRange);
		assertEquals(2, filesToRollup.size());
		Path rolledUpPath = Paths.get(segment.getPath().toString(), rollupRange.toUnderscoreDelimitedString());
		Path tmpPath = FileUtils.createTempFilePath(rolledUpPath);
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
	}

	@Test
	public void test_recover_pendingRollup_crash_before_deletes_finished() throws Exception {
		BlueKey key1 = createKey(1, 1);
		BlueKey key2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");

		getTimeCollection().insert(key1, value1);
		getTimeCollection().insert(key2, value2);
		Range rollupRange = getTimeCollection().getSegmentManager().getSegmentRange(0);
		RollupTarget rollupTarget = new RollupTarget(0, rollupRange);
		PendingRollup<TestValue> pendingRollup = new PendingRollup<TestValue>(rollupTarget);
		getRecoveryManager().saveChange(pendingRollup);

		Segment<TestValue> segment = getTimeCollection().getSegmentManager().getFirstSegment(key1);
		List<File> filesToRollup = segment.getOrderedFilesInRange(rollupRange);
		assertEquals(2, filesToRollup.size());
		Path rolledUpPath = Paths.get(segment.getPath().toString(), rollupRange.toUnderscoreDelimitedString());
		Path tmpPath = FileUtils.createTempFilePath(rolledUpPath);
		segment.copy(tmpPath, filesToRollup);


		File[] filesExistingAfterCopy = tmpPath.toFile().getParentFile().listFiles(); //segment.getOrderedFilesInRange(rollupRange);
		assertEquals(3, filesExistingAfterCopy.length);

		try (BlueWriteLock<Path> targetFileLock = getLockManager().acquireWriteLock(rolledUpPath)) {
			FileUtils.moveFile(tmpPath, targetFileLock);
		}
		File file = filesToRollup.get(0);
		try (BlueWriteLock<Path> writeLock = getLockManager().acquireWriteLock(file.toPath())) {
			FileUtils.deleteFile(writeLock);
		}

		getRecoveryManager().recover();
		List<File> remainingFiles = segment.getOrderedFilesInRange(rollupRange);
		assertEquals(1, remainingFiles.size());
		List<TestValue> values = getTimeCollection().query().getList();
		assertEquals(2, values.size());
		assertTrue(values.contains(value1));
		assertTrue(values.contains(value2));
	}

	@Test
	public void test_recover_pendingRollup_crash_after_completed() throws Exception {
		BlueKey key1 = createKey(1, 1);
		BlueKey key2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");

		getTimeCollection().insert(key1, value1);
		getTimeCollection().insert(key2, value2);
		Range rollupRange = getTimeCollection().getSegmentManager().getSegmentRange(0);
		RollupTarget rollupTarget = new RollupTarget(0, rollupRange);
		PendingRollup<TestValue> pendingRollup = new PendingRollup<TestValue>(rollupTarget);
		getRecoveryManager().saveChange(pendingRollup);

		Segment<TestValue> segment = getTimeCollection().getSegmentManager().getFirstSegment(key1);
		List<File> filesToRollup = segment.getOrderedFilesInRange(rollupRange);
		assertEquals(2, filesToRollup.size());
		Path rolledUpPath = Paths.get(segment.getPath().toString(), rollupRange.toUnderscoreDelimitedString());
		Path tmpPath = FileUtils.createTempFilePath(rolledUpPath);
		segment.copy(tmpPath, filesToRollup);


		File[] filesExistingAfterCopy = tmpPath.toFile().getParentFile().listFiles(); //segment.getOrderedFilesInRange(rollupRange);
		assertEquals(3, filesExistingAfterCopy.length);

		try (BlueWriteLock<Path> targetFileLock = getLockManager().acquireWriteLock(rolledUpPath)) {
			FileUtils.moveFile(tmpPath, targetFileLock);
		}
		for (File file: filesToRollup) {
			try (BlueWriteLock<Path> writeLock = getLockManager().acquireWriteLock(file.toPath())) {
				FileUtils.deleteFile(writeLock);
			}
		}

		getRecoveryManager().recover();
		List<File> remainingFiles = segment.getOrderedFilesInRange(rollupRange);
		assertEquals(1, remainingFiles.size());
		List<TestValue> values = getTimeCollection().query().getList();
		assertEquals(2, values.size());
		assertTrue(values.contains(value1));
		assertTrue(values.contains(value2));
	}

	@Test
	public void test_isValidRollupRange() {
		SegmentManager<TestValue> timeSegmentManager = getTimeCollection().getSegmentManager();
		SegmentManager<TestValue> valueSegmentManager = getHashGroupedCollection().getSegmentManager();
		Segment<TestValue> timeSegment = timeSegmentManager.getSegment(0);
		Segment<TestValue> valueSegment = valueSegmentManager.getSegment(0);
		long timeSegmentSize = timeSegmentManager.getSegmentSize();
		long valueSegmentSize = valueSegmentManager.getSegmentSize();
		Range validTimeSegmentRange = new Range(0, timeSegmentSize - 1);
		Range invalidTimeSegmentRange1 = new Range(1, timeSegmentSize);
		Range invalidTimeSegmentRange2 = new Range(0, timeSegmentSize);
		Range validValueSegmentRange = new Range(0, valueSegmentSize - 1);
		Range invalidValueSegmentRange1 = new Range(1, valueSegmentSize);
		Range invalidValueSegmentRange2 = new Range(0, valueSegmentSize);

		assertTrue(timeSegment.isValidRollupRange(validTimeSegmentRange));
		assertFalse(timeSegment.isValidRollupRange(invalidTimeSegmentRange1));
		assertFalse(timeSegment.isValidRollupRange(invalidTimeSegmentRange2));
		assertFalse(timeSegment.isValidRollupRange(validValueSegmentRange));
		assertFalse(timeSegment.isValidRollupRange(invalidValueSegmentRange1));
		assertFalse(timeSegment.isValidRollupRange(invalidValueSegmentRange2));

		assertFalse(valueSegment.isValidRollupRange(validTimeSegmentRange));
		assertFalse(valueSegment.isValidRollupRange(invalidTimeSegmentRange1));
		assertFalse(valueSegment.isValidRollupRange(invalidTimeSegmentRange2));
		assertTrue(valueSegment.isValidRollupRange(validValueSegmentRange));
		assertFalse(valueSegment.isValidRollupRange(invalidValueSegmentRange1));
		assertFalse(valueSegment.isValidRollupRange(invalidValueSegmentRange2));
	}

	@Test
	public void test_getRollupRanges() {
		Range segmentRange = new Range(0, 99);
		Segment<?> segment = new Segment<>(null, segmentRange, null, null, Arrays.asList(1L, 10L, 100L));
		Range _10_19 = new Range(10, 19);
		Range _0_99 = new Range(0, 99);
		Range _0_100 = new Range(0, 100);
		Range _9_11 = new Range(9, 11);
		Range _10_10 = new Range(10, 10);
		Range _10_11 = new Range(10, 11);
		
		List<Range> ranges_none = Arrays.asList();
		List<Range> ranges_10_100 = Arrays.asList(_10_19, _0_99);
		List<Range> ranges_100 = Arrays.asList(_0_99);

		List<Range> rangesFor9to11 = segment.getRollupRanges(_9_11);
		List<Range> rangesFor10to10 = segment.getRollupRanges(_10_10);
		List<Range> rangesFor10to11 = segment.getRollupRanges(_10_11);
		List<Range> rangesFor10to19 = segment.getRollupRanges(_10_19);
		List<Range> rangesFor0to99 = segment.getRollupRanges(_0_99);
		List<Range> rangesFor0to100 = segment.getRollupRanges(_0_100);
		
		assertEquals(ranges_100, rangesFor9to11);
		assertEquals(ranges_10_100, rangesFor10to10);
		assertEquals(ranges_10_100, rangesFor10to11);
		assertEquals(ranges_100, rangesFor10to19);
		assertEquals(ranges_none, rangesFor0to99);
		assertEquals(ranges_none, rangesFor0to100);
	}

	@Test
	public void test_getRollupTargets() {
		Range segmentRange = new Range(0, 99);
		Segment<?> segment = new Segment<>(null, segmentRange, null, null, Arrays.asList(1L, 10L, 100L));
		Range _10_19 = new Range(10, 19);
		Range _0_99 = new Range(0, 99);
		Range _0_100 = new Range(0, 100);
		Range _9_11 = new Range(9, 11);
		Range _10_10 = new Range(10, 10);
		Range _10_11 = new Range(10, 11);
		
		RollupTarget target_10_19 = new RollupTarget(segmentRange.getStart(), _10_19);
		RollupTarget target_10_99 = new RollupTarget(segmentRange.getStart(), _0_99);
		
		List<RollupTarget> targets_none = Arrays.asList();
		List<RollupTarget> targets_10_100 = Arrays.asList(target_10_19, target_10_99);
		List<RollupTarget> targets_100 = Arrays.asList(target_10_99);

		List<RollupTarget> rangesFor9to11 = segment.getRollupTargets(_9_11);
		List<RollupTarget> rangesFor10to10 = segment.getRollupTargets(_10_10);
		List<RollupTarget> rangesFor10to11 = segment.getRollupTargets(_10_11);
		List<RollupTarget> rangesFor10to19 = segment.getRollupTargets(_10_19);
		List<RollupTarget> rangesFor0to99 = segment.getRollupTargets(_0_99);
		List<RollupTarget> rangesFor0to100 = segment.getRollupTargets(_0_100);
		
		assertEquals(targets_100, rangesFor9to11);
		assertEquals(targets_10_100, rangesFor10to10);
		assertEquals(targets_10_100, rangesFor10to11);
		assertEquals(targets_100, rangesFor10to19);
		assertEquals(targets_none, rangesFor0to99);
		assertEquals(targets_none, rangesFor0to100);
	}
}
