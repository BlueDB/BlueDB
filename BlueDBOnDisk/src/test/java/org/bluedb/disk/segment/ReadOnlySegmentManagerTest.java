package org.bluedb.disk.segment;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.BlueDbOnDiskBuilder;
import org.bluedb.disk.ReadableDbOnDisk;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.ReadOnlyTimeCollectionOnDisk;
import org.junit.Test;

public class ReadOnlySegmentManagerTest extends BlueDbDiskTestBase {
	
	private ReadableDbOnDisk readOnlyDb;
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		this.readOnlyDb = (ReadableDbOnDisk) new BlueDbOnDiskBuilder().withPath(getPath()).buildReadOnly();
	}

	@Test
	public void test_getExistingSegments() throws BlueDbException {
		long minTime = 0;
		long segmentSize = getSegmentManager().getSegmentSize();
		long maxTime = segmentSize * 2;
		BlueKey timeFrameKey = new TimeFrameKey(1, minTime, maxTime);  // should barely span 3 segments
		TestValue value = new TestValue("Bob", 0);
		try {
			List<ReadOnlySegment<TestValue>> existingSegments = getSegmentManager().getExistingSegments(new Range(minTime, maxTime), Optional.empty());
			assertEquals(0, existingSegments.size());
			
			getTimeCollection().insert(timeFrameKey, value);
			existingSegments = getSegmentManager().getExistingSegments(new Range(minTime, maxTime), Optional.empty());
			assertEquals(3, existingSegments.size());
			
			Optional<Set<Range>> segmentRangesToInclude = Optional.of(existingSegments.stream()
				.map(segment -> segment.getRange())
				.collect(Collectors.toSet()));
			existingSegments = getSegmentManager().getExistingSegments(new Range(minTime, maxTime), segmentRangesToInclude);
			assertEquals(3, existingSegments.size());
			
			segmentRangesToInclude.get().remove(existingSegments.get(0).getRange());
			existingSegments = getSegmentManager().getExistingSegments(new Range(minTime, maxTime), segmentRangesToInclude);
			assertEquals(2, existingSegments.size());
			
			segmentRangesToInclude.get().remove(existingSegments.get(0).getRange());
			existingSegments = getSegmentManager().getExistingSegments(new Range(minTime, maxTime), segmentRangesToInclude);
			assertEquals(1, existingSegments.size());
			
			segmentRangesToInclude.get().remove(existingSegments.get(0).getRange());
			existingSegments = getSegmentManager().getExistingSegments(new Range(minTime, maxTime), segmentRangesToInclude);
			assertEquals(0, existingSegments.size());
			
			List<ReadOnlySegment<TestValue>> existingSegments0to0 = getSegmentManager().getExistingSegments(new Range(minTime, minTime), Optional.empty());
			assertEquals(1, existingSegments0to0.size());
			
			List<ReadOnlySegment<TestValue>> existingSegmentsOutsideRange = getSegmentManager().getExistingSegments(new Range(maxTime * 2, maxTime * 2), Optional.empty());
			assertEquals(0, existingSegmentsOutsideRange.size());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_getFirstSegment() throws BlueDbException {
		long segmentSize = getSegmentManager().getSegmentSize();
		BlueKey timeFrameKey = new TimeFrameKey(1, segmentSize, segmentSize * 2);
		assertEquals(new Range(3600000, 7199999), getSegmentManager().getFirstSegment(timeFrameKey).getRange());
	}

	@Test
	public void test_toSegment() throws BlueDbException {
		BlueKey key = new TimeKey(5, randomTime());
		Path path = getSegmentManager().getPathManager().getSegmentPath(key);
		ReadOnlySegment<TestValue> segment = getSegmentManager().toSegment(path);
		assertEquals(path, segment.getPath());
	}

	@Test
	public void test_toRange() throws BlueDbException {
		long segmentSize = getSegmentManager().getSegmentSize();
		Path firstPath = getSegmentManager().getPathManager().getSegmentPath(0);
		Path secondPath = getSegmentManager().getPathManager().getSegmentPath(segmentSize);
		Range firstRange = getSegmentManager().toRange(firstPath);
		Range secondRange = getSegmentManager().toRange(secondPath);
		Range expectedFirstRange = new Range(0, segmentSize - 1);
		Range expectedSecondRange = new Range(segmentSize, segmentSize * 2 - 1);
		assertEquals(expectedFirstRange, firstRange);
		assertEquals(expectedSecondRange, secondRange);
		assertEquals(firstRange.getEnd() + 1, secondRange.getStart());
	}

	private ReadOnlySegmentManager<TestValue> getSegmentManager() throws BlueDbException {
		ReadOnlyTimeCollectionOnDisk<TestValue> readOnlyTimeCollection = (ReadOnlyTimeCollectionOnDisk<TestValue>) readOnlyDb.getTimeCollection(TIME_COLLECTION_NAME, TestValue.class);
		return readOnlyTimeCollection.getSegmentManager();
	}


	private long randomTime() {
		long aboutOneHundredYears = 100 * 365 * 24 * 60 * 60 * 1000;
		return ThreadLocalRandom.current().nextLong(aboutOneHundredYears);
	}

}
