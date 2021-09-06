package org.bluedb.disk.segment;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mockito.Mockito;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.file.ReadWriteFileManager;
import org.bluedb.disk.file.FileUtils;
import org.bluedb.disk.lock.BlueWriteLock;
import org.bluedb.disk.recovery.InMemorySortedChangeSupplier;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.recovery.PendingRollup;
import org.bluedb.disk.recovery.SortedChangeSupplier;
import org.bluedb.disk.segment.rollup.RollupTarget;
import org.bluedb.disk.segment.rollup.Rollupable;

public class ReadWriteSegmentTest extends BlueDbDiskTestBase {

	@Test
	public void test_contains() throws Exception {
		ReadWriteSegment<TestValue> segment = getSegment();
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
	public void test_removedSegmentFolderDoesntCauseExceptions() throws Exception {
		Path segmentParentPath = createTempFolder().toPath();
		Path segmentPath = Paths.get(segmentParentPath.toString(), "nonexistingChildFolder");
		Range segmentRange = new Range(100, 200);
		Rollupable rollupable = Mockito.mock(ReadWriteCollectionOnDisk.class);
		ReadWriteFileManager fileManager = getFileManager();
		List<Long> rollupLevels = Arrays.asList(100L);
		ReadWriteSegment<TestValue> segment = new ReadWriteSegment<>(segmentPath, segmentRange, rollupable, fileManager, rollupLevels);

		// perform read actions on nonexisting folder to make sure that deleting empty folder during rollup won't break reads.
		segment.get(new TimeKey(1L, 1L));
		segment.contains(new TimeKey(1L, 1L));
		segment.getIterator(Long.MIN_VALUE, Long.MAX_VALUE).forEachRemaining((t) -> {});

		// make sure it didn't exist the whole time
		assertFalse(segment.getPath().toFile().exists());
	}

	@Test
	public void test_insert() throws Exception {
		ReadWriteSegment<TestValue> segment = getSegment();
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
		ReadWriteSegment<TestValue> segment = getSegment();
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
		ReadWriteSegment<TestValue> segment = getSegment();
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
		SortedChangeSupplier<TestValue> sortedChanges = new InMemorySortedChangeSupplier<TestValue>(changes, new Range(Long.MIN_VALUE, Long.MAX_VALUE));
		segment.applyChanges(sortedChanges);

		assertTrue(segment.contains(key1At1));
		assertTrue(segment.contains(key2At1));
		assertTrue(segment.contains(key3At3));
		assertEquals(value1, segment.get(key1At1));
		assertEquals(value2, segment.get(key2At1));
		assertEquals(value3, segment.get(key3At3));
	}

	@Test
	public void test_applyChanges_preBatchRollup() throws Exception {
		ReadWriteSegment<TestValue> segment = getSegment();
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key2At2 = createKey(2, 2);
		BlueKey key3At3 = createKey(3, 3);
		BlueKey keySegmentEnd = createKey(4, segment.getRange().getEnd());
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		TestValue value3 = createValue("Charlie");
		TestValue value4 = createValue("Daryl");

		List<TestValue> listEmpty = Arrays.asList();
		List<TestValue> list1 = Arrays.asList(value1);
		List<TestValue> list12And3 = Arrays.asList(value1, value2, value3);
		List<TestValue> list123And4 = Arrays.asList(value1, value2, value3, value4);

		assertEquals(listEmpty, getSegmentContents(segment));
		assertEquals(0, ReadWriteSegment.getAllFileRangesInOrder(segment.getPath()).size());

		segment.insert(key1At1, value1);
		assertEquals(list1, getSegmentContents(segment));
		assertEquals(1, ReadWriteSegment.getAllFileRangesInOrder(segment.getPath()).size());

		IndividualChange<TestValue> insert2At2 = IndividualChange.createInsertChange(key2At2, value2);
		IndividualChange<TestValue> insert4At4 = IndividualChange.createInsertChange(key3At3, value3);
		List<IndividualChange<TestValue>> changes = Arrays.asList(insert2At2, insert4At4);
		SortedChangeSupplier<TestValue> sortedChanges = new InMemorySortedChangeSupplier<TestValue>(changes, new Range(Long.MIN_VALUE, Long.MAX_VALUE));
		segment.applyChanges(sortedChanges);
		assertEquals(list12And3, getSegmentContents(segment));
		assertEquals(1, ReadWriteSegment.getAllFileRangesInOrder(segment.getPath()).size());

		IndividualChange<TestValue> inserinset3AtSegmentEnd = IndividualChange.createInsertChange(keySegmentEnd, value4);
		changes = Arrays.asList(inserinset3AtSegmentEnd);
		sortedChanges = new InMemorySortedChangeSupplier<TestValue>(changes, new Range(Long.MIN_VALUE, Long.MAX_VALUE));
		segment.applyChanges(sortedChanges);
		assertEquals(list123And4, getSegmentContents(segment));
		assertEquals(2, ReadWriteSegment.getAllFileRangesInOrder(segment.getPath()).size());
	}

	public static <T extends Serializable> List<T> getSegmentContents(ReadWriteSegment<T> segment) {
		List<T> results = new ArrayList<>();
		segment.getIterator(Long.MIN_VALUE, Long.MAX_VALUE).forEachRemaining((entity) -> {
			results.add(entity.getValue());
		});
		return results;
	}

	@Test
	public void testGet() throws Exception {
		ReadWriteSegment<TestValue> segment = getSegment();
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
		ReadWriteSegment<TestValue> firstSegment = getSegment(0);
		ReadWriteSegment<TestValue> secondSegment = getSegment(segmentSize);
		Range expectedFirstRange = new Range(0, segmentSize - 1);
		assertEquals(expectedFirstRange, firstSegment.getRange());
		assertEquals(firstSegment.getRange().getEnd() + 1, secondSegment.getRange().getStart());
	}

	@Test
	public void testGetAll() throws Exception {
		ReadWriteSegment<TestValue> segment = getSegment();
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
	public void test_rollup_previousData() throws Exception {
		long timeSegmentSize = getTimeCollection().getSegmentManager().getSegmentSize();
		ReadWriteSegment<TestValue> segment10 = getSegment(timeSegmentSize * 10);

		BlueKey key1 = createKey(1, 1*timeSegmentSize);
		BlueKey key2 = createKey(2, 2*timeSegmentSize);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");

		assertEquals(0, countItems(segment10));
		assertEquals(0, countFiles(segment10));

		segment10.insert(key1, value1);
		segment10.insert(key2, value2);
		assertEquals(2, countItems(segment10));
		assertEquals(2, countFiles(segment10));

		Range preSegmentRange = new Range(0, segment10.getRange().getStart() - 1);
		segment10.rollup(preSegmentRange);
		assertEquals(2, countItems(segment10));
		assertEquals(1, countFiles(segment10));
	}

	@Test
	public void test_rollup() throws Exception {
		ReadWriteSegment<TestValue> segment = getSegment();
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value3 = createValue("Chuck");
		List<TestValue> values;

		values = getAll(segment);
		assertEquals(0, values.size());

		segment.insert(key1At1, value1);
		segment.insert(key3At3, value3);
		assertEquals(2, countFiles(segment));
		assertEquals(2, countFiles(segment));

		Range invalidRollupTimeRange = new Range(0, 3);
		try {
			segment.rollup(invalidRollupTimeRange);
			fail();  // rollups must be
		} catch (BlueDbException e) {}

		Range validRollupTimeRange = new Range(0, getTimeCollection().getSegmentManager().getSegmentSize() - 1);
		segment.rollup(validRollupTimeRange);
		assertEquals(2, countItems(segment));
		assertEquals(1, countFiles(segment));
	}

	@Test
	public void test_rollup_removeEmptyFiles() throws Exception {
		ReadWriteSegment<TestValue> segment = getSegment();
		BlueKey key1At1 = createKey(1, 1);
		TestValue value1 = createValue("Anna");

		segment.insert(key1At1, value1);
		assertEquals(1, segment.getPath().toFile().listFiles().length);

		segment.delete(key1At1);
		assertEquals(1, segment.getPath().toFile().listFiles().length);  // empty file still exists until rollup

		Range rollupRange = new Range(0, getTimeCollection().getSegmentManager().getSegmentSize() - 1);
		segment.rollup(rollupRange);
		assertFalse(segment.getPath().toFile().exists()); // empty folder should get deleted
	}

	@Test
	public void test_rollup_removeEmptyFilesButLeaveNonEmpty() throws Exception {
		ReadWriteSegment<TestValue> segment = getSegment();
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value3 = createValue("Chuck");
		List<TestValue> listEmpty = Arrays.asList();
		List<TestValue> list1and3 = Arrays.asList(value1, value3);
		List<TestValue> list3 = Arrays.asList(value3);

		assertEquals(listEmpty, getAll(segment));

		segment.insert(key1At1, value1);
		segment.insert(key3At3, value3);
		assertEquals(2, segment.getPath().toFile().listFiles().length);
		assertEquals(list1and3, getAll(segment));

		segment.delete(key1At1);
		assertEquals(2, segment.getPath().toFile().listFiles().length);  // empty file still exists until rollup
		assertEquals(list3, getAll(segment));

		Range rollupRange = new Range(0, getTimeCollection().getSegmentManager().getSegmentSize() - 1);
		segment.rollup(rollupRange);
		assertEquals(1, segment.getPath().toFile().listFiles().length);
		assertEquals(list3, getAll(segment));
	}

	@Test
	public void test_rollup_out_of_order() throws Exception {
		ReadWriteSegment<TestValue> segment = getSegment();
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
		
		assertEquals(expected, ReadWriteSegment.getAllFileRangesInOrder(getPath()));
	}

	@Test
	public void testToString() {
		ReadWriteSegment<TestValue> segment = getSegment();
		assertTrue(segment.toString().contains(segment.getPath().toString()));
		assertTrue(segment.toString().contains(segment.getClass().getSimpleName()));
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void test_equals() {
		ReadWriteSegment<TestValue> segment1 = getSegment(1);
		ReadWriteSegment<TestValue> segment1copy = getSegment(1);
		ReadWriteSegment<TestValue> segmentMax = getSegment(Long.MAX_VALUE);
		ReadableSegment<TestValue> segmentNullPath = ReadWriteSegment.getTestSegment();
		ReadableSegment<TestValue> segmentNullPathCopy = ReadWriteSegment.getTestSegment();
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
		ReadWriteSegment<TestValue> segment1 = getSegment(1);
		ReadWriteSegment<TestValue> segment1copy = getSegment(1);
		ReadWriteSegment<TestValue> segmentMax = getSegment(Long.MAX_VALUE);
		ReadableSegment<TestValue> segmentNullPath = ReadWriteSegment.getTestSegment();
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

		ReadWriteSegment<TestValue> segment = getTimeCollection().getSegmentManager().getFirstSegment(key1);

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

		ReadWriteSegment<TestValue> segment = getTimeCollection().getSegmentManager().getFirstSegment(key1);
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

		ReadWriteSegment<TestValue> segment = getTimeCollection().getSegmentManager().getFirstSegment(key1);
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

		ReadWriteSegment<TestValue> segment = getTimeCollection().getSegmentManager().getFirstSegment(key1);
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
		ReadWriteSegmentManager<TestValue> timeSegmentManager = getTimeCollection().getSegmentManager();
		ReadWriteSegmentManager<TestValue> valueSegmentManager = getHashGroupedCollection().getSegmentManager();
		ReadWriteSegment<TestValue> timeSegment = timeSegmentManager.getSegment(0);
		ReadWriteSegment<TestValue> valueSegment = valueSegmentManager.getSegment(0);
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
	public void test_isValidRollupRange_preSegment() {
		ReadWriteSegmentManager<TestValue> timeSegmentManager = getTimeCollection().getSegmentManager();
		ReadWriteSegmentManager<TestValue> valueSegmentManager = getHashGroupedCollection().getSegmentManager();
		long timeSegmentSize = timeSegmentManager.getSegmentSize();
		long valueSegmentSize = valueSegmentManager.getSegmentSize();
		ReadWriteSegment<TestValue> timeSegment = timeSegmentManager.getSegment(timeSegmentSize * 10);
		ReadWriteSegment<TestValue> valueSegment = valueSegmentManager.getSegment(valueSegmentSize * 10);
		long timeSegmentStart = timeSegment.getRange().getStart();
		long valueSegmentStart = timeSegment.getRange().getStart();
		Range validTimeSegmentRange = new Range(0, timeSegmentStart - 1);
		Range invalidTimeSegmentRange1 = new Range(1, timeSegmentStart - 1);
		Range invalidTimeSegmentRange2 = new Range(0, timeSegmentStart);
		Range invalidTimeSegmentRange3 = new Range(0, timeSegmentStart - timeSegmentSize - 1);
		Range validValueSegmentRange = new Range(0, valueSegmentSize - 1);
		Range invalidValueSegmentRange1 = new Range(1, valueSegmentStart - 1);
		Range invalidValueSegmentRange2 = new Range(0, valueSegmentStart);

		assertTrue(timeSegment.isValidRollupRange(validTimeSegmentRange));
		assertFalse(timeSegment.isValidRollupRange(invalidTimeSegmentRange1));
		assertFalse(timeSegment.isValidRollupRange(invalidTimeSegmentRange2));
		assertFalse(timeSegment.isValidRollupRange(invalidTimeSegmentRange3));

		assertTrue(valueSegment.isValidRollupRange(validValueSegmentRange));
		assertFalse(valueSegment.isValidRollupRange(invalidValueSegmentRange1));
		assertFalse(valueSegment.isValidRollupRange(invalidValueSegmentRange2));
	}

	@Test
	public void test_getRollupRanges() {
		Range segmentRange = new Range(0, 99);
		ReadWriteSegment<?> segment = new ReadWriteSegment<>(null, segmentRange, null, null, Arrays.asList(1L, 10L, 100L));
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
		ReadWriteSegment<?> segment = new ReadWriteSegment<>(null, segmentRange, null, null, Arrays.asList(1L, 10L, 100L));
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
	
	@Test
	public void test_calculatePossibleChunkRanges() {
		ReadWriteSegment<TestValue> segment = new ReadWriteSegment<>(null, new Range(120, 179), null, null, Arrays.asList(5l, 10l, 30l, 60l));
		
		Set<Range> expectedFor120 = new HashSet<>(Arrays.asList(
				new Range(120, 124), 
				new Range(120, 129), 
				new Range(120, 149), 
				new Range(120, 179)
		));
		
		Set<Range> expectedFor137 = new HashSet<>(Arrays.asList(
				new Range(135, 139), 
				new Range(130, 139), 
				new Range(120, 149), 
				new Range(120, 179)
		));
		
		Set<Range> expectedFor179 = new HashSet<>(Arrays.asList(
				new Range(175, 179), 
				new Range(170, 179), 
				new Range(150, 179), 
				new Range(120, 179)
		));
		
		Set<Range> expectedFor72 = new HashSet<>(Arrays.asList(
				new Range(70, 74), 
				new Range(70, 79), 
				new Range(60, 89), 
				new Range(60, 119),
				new Range(0, 119) //Pre segment chunk range
		));
		
		assertEquals(expectedFor120, new HashSet<>(segment.calculatePossibleChunkRanges(120)));
		assertEquals(expectedFor137, new HashSet<>(segment.calculatePossibleChunkRanges(137)));
		assertEquals(expectedFor179, new HashSet<>(segment.calculatePossibleChunkRanges(179)));
		assertEquals(expectedFor72, new HashSet<>(segment.calculatePossibleChunkRanges(72)));
	}

	@Test
	public void test_batchUpdateIntoInfinitelyRolledUpPreSegmentChunk() throws BlueDbException {
		assertEquals(TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS), getTimeSegmentManager().getPathManager().getSegmentSize());
		
		long now = System.currentTimeMillis();
		long fiveHoursAgo = now - TimeUnit.MILLISECONDS.convert(5, TimeUnit.HOURS);
		long threeHoursAgo = now - TimeUnit.MILLISECONDS.convert(3, TimeUnit.HOURS);
		long twoHoursInFuture = now + TimeUnit.MILLISECONDS.convert(2, TimeUnit.HOURS);
		long threeHoursInFuture = now + TimeUnit.MILLISECONDS.convert(3, TimeUnit.HOURS);
		
		TestValue derek = new TestValue("Derek", 0);
		TestValue jeremy = new TestValue("Jeremy", 2);
		TestValue ben = new TestValue("Ben", 5);
		TestValue weston = new TestValue("Weston", 4);
		
		insertAtTimeFrame(fiveHoursAgo, twoHoursInFuture, derek);
		insertAtTimeFrame(threeHoursAgo, threeHoursInFuture, jeremy);
		insertAtTimeFrame(now, now + 10, ben);

		getTimeCollection().getRollupScheduler().forceScheduleRollups();
		insertAtTimeFrame(now + 20, now + 50, weston); //Shouldn't complete until rollups are done
		
		getTimeCollection().query()
			.update(value -> value.addCupcake());
		
		assertEquals(1, getTimeCollection().query().where(value -> "Derek".equals(value.getName())).getList().get(0).getCupcakes());
		assertEquals(3, getTimeCollection().query().where(value -> "Jeremy".equals(value.getName())).getList().get(0).getCupcakes());
		assertEquals(6, getTimeCollection().query().where(value -> "Ben".equals(value.getName())).getList().get(0).getCupcakes());
		assertEquals(5, getTimeCollection().query().where(value -> "Weston".equals(value.getName())).getList().get(0).getCupcakes());
	}

	@Test
	public void test_tryReportRead() {
		ReadWriteSegment<TestValue> segment = new ReadWriteSegment<>(null, new Range(0, 1), null, null, Arrays.asList());
		segment.tryReportRead(null);  // make sure we catch exception instead so lock doesn't get created and not released
		
		segment.tryReportRead(Paths.get("0_1"));
	}

}
