package io.bluedb.disk.segment.rollup;

import java.io.File;
import java.util.List;

import org.junit.Test;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.LongKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.segment.Range;
import io.bluedb.disk.segment.Segment;
import io.bluedb.disk.segment.SegmentManager;

public class RollupTaskTest extends BlueDbDiskTestBase {

	@Test
	public void test_rollup() throws BlueDbException {
		BlueCollectionOnDisk<TestValue> collection = getTimeCollection();
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
		
		Segment<TestValue> segment = collection.getSegmentManager().getSegment(key1At1.getGroupingNumber());
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
		BlueCollectionOnDisk<TestValue> collection = getTimeCollection();
		SegmentManager<TestValue> segmentManager = collection.getSegmentManager();
		
		Segment<TestValue> segment0 = segmentManager.getSegment(0);
		Range segment0range = segment0.getRange();
		long startOfNextSegment = segment0range.getEnd() + 1;
		Segment<TestValue> segmentX = segmentManager.getSegment(startOfNextSegment);
		
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
		
		File segmentFolder = segmentX.getPath().toFile();
		File[] segmentXDirectoryContents = segmentFolder.listFiles();
		assertEquals(2, segmentXDirectoryContents.length);

		RollupTarget segment0rollupTarget = new RollupTarget(0, segment0range);
		RollupTarget segmentXrollupTarget = new RollupTarget(startOfNextSegment, segment0range);
		RollupTask<TestValue> rollup0task = new RollupTask<>(collection, segment0rollupTarget);
		RollupTask<TestValue> rollupXtask = new RollupTask<>(collection, segmentXrollupTarget);

		rollup0task.run();
		segmentXDirectoryContents = segmentFolder.listFiles();
		assertEquals(2, segmentXDirectoryContents.length);

		rollupXtask.run();
		values = collection.query().afterOrAtTime(startOfNextSegment).getList();
		assertEquals(2, values.size());
		segmentXDirectoryContents = segmentFolder.listFiles();
		assertEquals(1, segmentXDirectoryContents.length);
	}
}
