package io.bluedb.disk.collection;

import org.junit.Test;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.serialization.BlueEntity;

public class LastEntityFinderTest extends BlueDbDiskTestBase {

	@Test
	public void test_empty() {
		LastEntityFinder<TestValue> lastFinder = new LastEntityFinder<TestValue>(getTimeCollection());
		BlueEntity<TestValue> lastEntity = lastFinder.getLastEntity();
		assertNull(lastEntity);
	}

	@Test
	public void test_singleSegment() {
		long segmentSize = getTimeCollection().getSegmentManager().getSegmentSize();
		
		TestValue value1 = new TestValue("value1");
		TestValue value2 = new TestValue("value2");
		insertAtTimeFrame(0, 1, value1);
		insertAtTimeFrame(1, 2, value2);

		LastEntityFinder<TestValue> lastFinder = new LastEntityFinder<TestValue>(getTimeCollection());
		BlueEntity<TestValue> lastEntity = lastFinder.getLastEntity();
		assertNotNull(lastEntity);
		assertEquals(value2, lastEntity.getValue());
	}

	@Test
	public void test_deletedItem() throws Exception {
		long segmentSize = getTimeCollection().getSegmentManager().getSegmentSize();
		
		TestValue valueInFirstSegment = new TestValue("first");
		TestValue valueInSecondSegment = new TestValue("second");
		BlueKey key1 = insertAtTimeFrame(0, 1, valueInFirstSegment);
		BlueKey key2 = insertAtTimeFrame(segmentSize, segmentSize + 1, valueInSecondSegment);

		LastEntityFinder<TestValue> lastFinder = new LastEntityFinder<TestValue>(getTimeCollection());
		BlueEntity<TestValue> lastEntity = lastFinder.getLastEntity();
		assertNotNull(lastEntity);
		assertEquals(valueInSecondSegment, lastEntity.getValue());

		getTimeCollection().delete(key2);

		lastFinder = new LastEntityFinder<TestValue>(getTimeCollection());
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

		LastEntityFinder<TestValue> lastFinder = new LastEntityFinder<TestValue>(getTimeCollection());
		BlueEntity<TestValue> lastEntity = lastFinder.getLastEntity();
		assertNotNull(lastEntity);
		assertEquals(valueAfterSecondSegment, lastEntity.getValue());
	}
}
