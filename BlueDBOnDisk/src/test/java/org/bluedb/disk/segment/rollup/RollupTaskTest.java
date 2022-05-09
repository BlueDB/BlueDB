package org.bluedb.disk.segment.rollup;

import java.io.File;
import java.util.List;

import org.bluedb.api.BlueCollectionVersion;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.ReadWriteTimeCollectionOnDisk;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadWriteSegment;
import org.bluedb.disk.segment.ReadWriteSegmentManager;
import org.junit.Test;

public class RollupTaskTest extends BlueDbDiskTestBase {

	@Test
	public void test_rollup() throws BlueDbException {
		ReadWriteTimeCollectionOnDisk<TestValue> collection = getTimeCollection();
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value3 = createValue("Chuck");
		List<TestValue> values;

		values = collection.query().getList();
		assertEquals(0, values.size());

		collection.insert(key1At1, value1);
		collection.insert(key3At3, value3);
		values = collection.query().getList();
		assertEquals(2, values.size());
		
		ReadWriteSegment<TestValue> segment = collection.getSegmentManager().getSegment(key1At1.getGroupingNumber());
		File segmentFolder = segment.getPath().toFile();
		File[] segmentDirectoryContents = segmentFolder.listFiles();
		assertEquals(2, segmentDirectoryContents.length);

		long segmentSize = collection.getSegmentManager().getSegmentSize();
		Range offByOneSegmentTimeRange = new Range(0, segmentSize);
		Range entireFirstSegmentTimeRange = new Range(0, segmentSize -1);
		RollupTarget offByOneRollupTarget = new RollupTarget(0, offByOneSegmentTimeRange);
		RollupTarget entireFirstRollupTarget = new RollupTarget(0, entireFirstSegmentTimeRange);
		RollupTask<TestValue> invalidRollup = new RollupTask<>(collection, offByOneRollupTarget);
		RollupTask<TestValue> validRollup = new RollupTask<>(collection, entireFirstRollupTarget);

		invalidRollup.run();
		segmentDirectoryContents = segmentFolder.listFiles();
		assertEquals(2, segmentDirectoryContents.length);

		validRollup.run();
		values = collection.query().getList();
		assertEquals(2, values.size());
		segmentDirectoryContents = segmentFolder.listFiles();
		assertEquals(1, segmentDirectoryContents.length);
	}


	@Test
	public void test_rollup_cross_segment() throws BlueDbException {
		ReadWriteTimeCollectionOnDisk<TestValue> collection = getTimeCollection();
		ReadWriteSegmentManager<TestValue> segmentManager = collection.getSegmentManager();
		
		ReadWriteSegment<TestValue> segment0 = segmentManager.getSegment(0);
		Range segment0range = segment0.getRange();
		long startOfNextSegment = segment0range.getEnd() + 1;
		ReadWriteSegment<TestValue> segmentX = segmentManager.getSegment(startOfNextSegment);
		
		segmentManager.getSegmentSize();
		BlueKey key1toX = new TimeFrameKey(new LongKey(1), 1, startOfNextSegment);
		BlueKey key2toX = new TimeFrameKey(new LongKey(2), 2, startOfNextSegment);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		List<TestValue> values;

		values = collection.query().afterOrAtTime(startOfNextSegment).getList();
		assertEquals(0, values.size());

		collection.insert(key1toX, value1);
		collection.insert(key2toX, value2);
		values = collection.query().afterOrAtTime(startOfNextSegment).getList();
		assertEquals(2, values.size());
		
		if(BlueCollectionVersion.getDefault() == BlueCollectionVersion.VERSION_1) {
			assertEquals(2, segmentX.getPath().toFile().listFiles().length);
	
			RollupTarget segment0rollupTarget = new RollupTarget(0, segment0range);
			RollupTarget segmentXrollupTarget = new RollupTarget(startOfNextSegment, segment0range);
			RollupTask<TestValue> rollup0task = new RollupTask<>(collection, segment0rollupTarget);
			RollupTask<TestValue> rollupXtask = new RollupTask<>(collection, segmentXrollupTarget);
	
			rollup0task.run();
			assertEquals(2, segmentX.getPath().toFile().listFiles().length);
	
			rollupXtask.run();
			values = collection.query().afterOrAtTime(startOfNextSegment).getList();
			assertEquals(2, values.size());
			assertEquals(1, segmentX.getPath().toFile().listFiles().length);
		} else {
			//Version 2 and on don't store duplicate records in each segment they overlap, so the results will be a bit different
			assertEquals(2, segment0.getPath().toFile().listFiles().length);
			assertNull(segmentX.getPath().toFile().listFiles());
	
			RollupTarget segment0rollupTarget = new RollupTarget(0, segment0range);
			RollupTarget segmentXrollupTarget = new RollupTarget(startOfNextSegment, segment0range);
			RollupTask<TestValue> rollup0task = new RollupTask<>(collection, segment0rollupTarget);
			RollupTask<TestValue> rollupXtask = new RollupTask<>(collection, segmentXrollupTarget);
	
			rollupXtask.run();
			assertEquals(2, segment0.getPath().toFile().listFiles().length);
	
			rollup0task.run();
			values = collection.query().afterOrAtTime(startOfNextSegment).getList();
			assertEquals(2, values.size());
			assertEquals(1, segment0.getPath().toFile().listFiles().length);
		}
	}

	@Test
	public void test_toString() {
		long rangeStart = 51;
		long rangeEnd = 61;
		Range range = new Range(rangeStart, rangeEnd);
		long segmentGroupingNumber = 71;
		RollupTarget target = new RollupTarget(segmentGroupingNumber, range);
		RollupTask<?> task = new RollupTask<>(null, target);
		String taskString = task.toString();
		assertTrue(taskString.contains(String.valueOf(rangeStart)));
		assertTrue(taskString.contains(String.valueOf(rangeEnd)));
		assertTrue(taskString.contains(String.valueOf(segmentGroupingNumber)));
		assertTrue(taskString.contains(task.getClass().getSimpleName()));
	}
}
