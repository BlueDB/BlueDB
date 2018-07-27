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

public class RollupTaskTest extends BlueDbDiskTestBase {

	@Test
	public void test_rollup() {
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value3 = createValue("Chuck");
		List<TestValue> values;
		try {
			values = getTimeCollection().query().getList();
			assertEquals(0, values.size());

			getTimeCollection().insert(key1At1, value1);
			getTimeCollection().insert(key3At3, value3);
			values = getTimeCollection().query().getList();
			assertEquals(2, values.size());
			
			Segment<TestValue> segment = getTimeCollection().getSegmentManager().getSegment(key1At1.getGroupingNumber());
			File[] segmentDirectoryContents = segment.getPath().toFile().listFiles();
			assertEquals(2, segmentDirectoryContents.length);

			long segmentSize = getTimeCollection().getSegmentManager().getSegmentSize();
			Range offByOneSegmentTimeRange = new Range(0, segmentSize);
			Range entireFirstSegmentTimeRange = new Range(0, segmentSize -1);
			RollupTask<TestValue> invalidRollup = new RollupTask<>(getTimeCollection(), offByOneSegmentTimeRange);
			RollupTask<TestValue> validRollup = new RollupTask<>(getTimeCollection(), entireFirstSegmentTimeRange);

			invalidRollup.run();
			segmentDirectoryContents = segment.getPath().toFile().listFiles();
			assertEquals(2, segmentDirectoryContents.length);

			validRollup.run();
			values = getTimeCollection().query().getList();
			assertEquals(2, values.size());
			segmentDirectoryContents = segment.getPath().toFile().listFiles();
			assertEquals(1, segmentDirectoryContents.length);

		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}
}
