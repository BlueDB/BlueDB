package org.bluedb.disk.segment.rollup;

import java.io.File;
import java.util.List;

import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.BlueTimeCollectionOnDisk;
import org.bluedb.disk.collection.index.BlueIndexOnDisk;
import org.bluedb.disk.collection.index.IndexRollupTask;
import org.bluedb.disk.collection.index.TestRetrievalKeyExtractor;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.Segment;
import org.junit.Test;

public class IndexRollupTaskTest extends BlueDbDiskTestBase {

	@Test
	public void test_rollup() throws Exception {
		TestRetrievalKeyExtractor keyExtractor = new TestRetrievalKeyExtractor();
		BlueTimeCollectionOnDisk<TestValue> collection = getTimeCollection();
		String indexName = "test_index";
		BlueIndex<IntegerKey, TestValue> index = collection.createIndex(indexName, IntegerKey.class, keyExtractor);
		BlueIndexOnDisk<IntegerKey, TestValue> indexOnDisk = (BlueIndexOnDisk<IntegerKey, TestValue>) index;

		BlueKey key1At1 = createKey(1, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna", 1);
		TestValue value3 = createValue("Chuck", 3);
		BlueKey retrievalKey1 = keyExtractor.extractKeys(value1).get(0);
		List<TestValue> values;

		values = collection.query().getList();
		assertEquals(0, values.size());

		collection.insert(key1At1, value1);
		collection.insert(key3At3, value3);
		values = collection.query().getList();
		assertEquals(2, values.size());

		Segment<?> indexSegment = indexOnDisk.getSegmentManager().getSegment(retrievalKey1.getGroupingNumber());
		File segmentFolder = indexSegment.getPath().toFile();
		File[] segmentDirectoryContents = segmentFolder.listFiles();
		assertEquals(2, segmentDirectoryContents.length);

		Range entireFirstSegmentTimeRange = indexSegment.getRange();
		Range offByOneSegmentTimeRange = new Range(entireFirstSegmentTimeRange.getStart(), entireFirstSegmentTimeRange.getEnd() + 1);
		IndexRollupTarget offByOneRollupTarget = new IndexRollupTarget(indexName, 0, offByOneSegmentTimeRange);
		IndexRollupTarget entireFirstRollupTarget = new IndexRollupTarget(indexName, 0, entireFirstSegmentTimeRange);
		IndexRollupTask<TestValue> invalidRollup = new IndexRollupTask<TestValue>(collection, offByOneRollupTarget);
		IndexRollupTask<TestValue> validRollup = new IndexRollupTask<>(collection, entireFirstRollupTarget);

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
	public void test_toString() {
		String indexName = "indexName";
		long rangeStart = 51;
		long rangeEnd = 61;
		Range range = new Range(rangeStart, rangeEnd);
		long segmentGroupingNumber = 71;
		IndexRollupTarget target = new IndexRollupTarget(indexName, segmentGroupingNumber, range);
		IndexRollupTask<?> task = new IndexRollupTask<>(null, target);
		String taskString = task.toString();
		assertTrue(taskString.contains(indexName));
		assertTrue(taskString.contains(String.valueOf(rangeStart)));
		assertTrue(taskString.contains(String.valueOf(rangeEnd)));
		assertTrue(taskString.contains(String.valueOf(segmentGroupingNumber)));
		assertTrue(taskString.contains(task.getClass().getSimpleName()));
	}
}
