package io.bluedb.disk.segment.rollup;

import java.io.File;
import java.util.List;

import org.junit.Test;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.segment.Range;
import io.bluedb.disk.segment.Segment;
import io.bluedb.disk.segment.SegmentManager;

public class RollupTaskTest extends BlueDbDiskTestBase {

	@Test
	public void test_rollup() {
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value3 = createValue("Chuck");
		List<TestValue> values;
		try {
			values = getCollection().query().getList();
			assertEquals(0, values.size());

			getCollection().insert(key1At1, value1);
			getCollection().insert(key3At3, value3);
			values = getCollection().query().getList();
			assertEquals(2, values.size());
			
			Segment<TestValue> segment = getCollection().getSegmentManager().getSegment(key1At1.getGroupingNumber());
			File[] segmentDirectoryContents = segment.getPath().toFile().listFiles();
			assertEquals(2, segmentDirectoryContents.length);

			long segmentSize = SegmentManager.getSegmentSize();
			Range offByOneSegmentTimeRange = new Range(0, segmentSize);
			Range entireFirstSegmentTimeRange = new Range(0, segmentSize -1);
			RollupTask<TestValue> invalidRollup = new RollupTask<>(getCollection(), offByOneSegmentTimeRange);
			RollupTask<TestValue> validRollup = new RollupTask<>(getCollection(), entireFirstSegmentTimeRange);

			invalidRollup.run();
			segmentDirectoryContents = segment.getPath().toFile().listFiles();
			assertEquals(2, segmentDirectoryContents.length);

			validRollup.run();
			values = getCollection().query().getList();
			assertEquals(2, values.size());
			segmentDirectoryContents = segment.getPath().toFile().listFiles();
			assertEquals(1, segmentDirectoryContents.length);

		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}
}
