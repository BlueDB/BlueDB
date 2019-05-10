package org.bluedb.disk.collection;

import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.serialization.BlueEntity;
import org.junit.Test;

public class LastEntityFinderTest extends BlueDbDiskTestBase {

	@Test
	public void test_empty() {
		LastEntityFinder lastFinder = new LastEntityFinder(getTimeCollection());
		BlueEntity<?> lastEntity = lastFinder.getLastEntity();
		assertNull(lastEntity);
	}

	@Test
	public void test_singleSegment() {
		TestValue value1 = new TestValue("value1");
		TestValue value2 = new TestValue("value2");
		insertAtTimeFrame(0, 1, value1);
		insertAtTimeFrame(1, 2, value2);

		LastEntityFinder lastFinder = new LastEntityFinder(getTimeCollection());
		BlueEntity<?> lastEntity = lastFinder.getLastEntity();
		assertNotNull(lastEntity);
		assertEquals(value2, lastEntity.getValue());
	}

	@Test
	public void test_deletedItem() throws Exception {
		long segmentSize = getTimeCollection().getSegmentManager().getSegmentSize();
		
		TestValue valueInFirstSegment = new TestValue("first");
		TestValue valueInSecondSegment = new TestValue("second");
		insertAtTimeFrame(0, 1, valueInFirstSegment);
		BlueKey key2 = insertAtTimeFrame(segmentSize, segmentSize + 1, valueInSecondSegment);

		LastEntityFinder lastFinder = new LastEntityFinder(getTimeCollection());
		BlueEntity<?> lastEntity = lastFinder.getLastEntity();
		assertNotNull(lastEntity);
		assertEquals(valueInSecondSegment, lastEntity.getValue());

		getTimeCollection().delete(key2);

		lastFinder = new LastEntityFinder(getTimeCollection());
		lastEntity = lastFinder.getLastEntity();
		assertNotNull(lastEntity);
		assertEquals(valueInFirstSegment, lastEntity.getValue());
	}

	@Test
	public void test_multipleSegments() {
		long segmentSize = getTimeCollection().getSegmentManager().getSegmentSize();
		
		TestValue valueInFirstSegment = new TestValue("first");
		TestValue valueInBothSegments = new TestValue("both");
		TestValue valueInSecondSegment = new TestValue("second");
		TestValue valueAfterSecondSegment = new TestValue("after");
		insertAtTimeFrame(0, 1, valueInFirstSegment);
		insertAtTimeFrame(1, segmentSize, valueInBothSegments);
		insertAtTimeFrame(segmentSize, segmentSize + 1, valueInSecondSegment);
		insertAtTimeFrame(segmentSize * 2, segmentSize * 2 + 1, valueAfterSecondSegment);

		LastEntityFinder lastFinder = new LastEntityFinder(getTimeCollection());
		BlueEntity<?> lastEntity = lastFinder.getLastEntity();
		assertNotNull(lastEntity);
		assertEquals(valueAfterSecondSegment, lastEntity.getValue());
	}
}
