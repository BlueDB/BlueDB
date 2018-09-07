package io.bluedb.disk.segment.rollup;

import java.io.File;
import java.util.List;

import org.junit.Test;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.index.BlueIndex;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.IntegerKey;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.collection.index.BlueIndexOnDisk;
import io.bluedb.disk.collection.index.IndexRollupTask;
import io.bluedb.disk.collection.index.TestRetrievalKeyExtractor;
import io.bluedb.disk.segment.Range;
import io.bluedb.disk.segment.Segment;

public class IndexRollupTaskTest extends BlueDbDiskTestBase {

	@Test
	public void test_rollup() throws Exception {
		TestRetrievalKeyExtractor keyExtractor = new TestRetrievalKeyExtractor();
		BlueCollectionOnDisk<TestValue> collection = getTimeCollection();
		String indexName = "test_index";
		BlueIndex<IntegerKey, TestValue> index = collection.createIndex(indexName, IntegerKey.class, keyExtractor);
		BlueIndexOnDisk<IntegerKey, TestValue> indexOnDisk = (BlueIndexOnDisk<IntegerKey, TestValue>) index;

		BlueKey key1At1 = createKey(1, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna", 1);
		TestValue value3 = createValue("Chuck", 3);
		BlueKey retrievalKey1 = keyExtractor.extractKeys(value1).get(0);
		List<TestValue> values;
		try {
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
			segmentDirectoryContents = indexSegment.getPath().toFile().listFiles();
			assertEquals(2, segmentDirectoryContents.length);

			validRollup.run();
			values = collection.query().getList();
			assertEquals(2, values.size());
			segmentDirectoryContents = indexSegment.getPath().toFile().listFiles();
			assertEquals(1, segmentDirectoryContents.length);

		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}
}
