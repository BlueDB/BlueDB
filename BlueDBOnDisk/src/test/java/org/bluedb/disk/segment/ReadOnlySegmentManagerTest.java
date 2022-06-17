package org.bluedb.disk.segment;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import org.bluedb.api.BlueCollectionVersion;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.BlueDbOnDiskBuilder;
import org.bluedb.disk.ReadableDbOnDisk;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.ReadOnlyTimeCollectionOnDisk;
import org.bluedb.disk.collection.index.conditions.IncludedSegmentRangeInfo;
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
		
		BlueKey timeFrameKey1 = new TimeFrameKey(1, minTime, maxTime);  // should barely span 3 segments
		TestValue value1 = new TestValue("Bob", 0);
		
		BlueKey timeFrameKey2 = new TimeFrameKey(2, minTime + segmentSize, maxTime); //In Segment 2
		TestValue value2 = new TestValue("Frank", 1);
		
		BlueKey timeFrameKey3 = new TimeFrameKey(3, maxTime, maxTime);  //In Segment 3
		TestValue value3 = new TestValue("Joe", 2);
		
		try {
			List<ReadOnlySegment<TestValue>> existingSegments = getSegmentManager().getExistingSegments(new Range(minTime, maxTime), Optional.empty());
			List<Range> existingSegmentRanges = getSegmentManager().getExistingSegmentRanges(new Range(minTime, maxTime), Optional.empty());
			assertEquals(0, existingSegments.size());
			assertEquals(0, existingSegmentRanges.size());
			
			getTimeCollection().insert(timeFrameKey1, value1);
			
			existingSegments = getSegmentManager().getExistingSegments(new Range(minTime, maxTime), Optional.empty());
			existingSegmentRanges = getSegmentManager().getExistingSegmentRanges(new Range(minTime, maxTime), Optional.empty());
			if(BlueCollectionVersion.getDefault() != BlueCollectionVersion.VERSION_1) {
				/*
				 * Version 2 and on won't store a single value in multiple segments, so verify there is only one segment and
				 * add additional records so that it brings it up to the expected count of 3.
				 */
				assertEquals(1, existingSegments.size());
				assertEquals(1, existingSegmentRanges.size());
				getTimeCollection().insert(timeFrameKey2, value2);
				getTimeCollection().insert(timeFrameKey3, value3);
			}
			
			existingSegments = getSegmentManager().getExistingSegments(new Range(minTime, maxTime), Optional.empty());
			existingSegmentRanges = getSegmentManager().getExistingSegmentRanges(new Range(minTime, maxTime), Optional.empty());
			assertEquals(3, existingSegments.size());
			assertEquals(3, existingSegmentRanges.size());
			
			Optional<IncludedSegmentRangeInfo> includedSegmentRangeInfo = Optional.of(new IncludedSegmentRangeInfo());
			existingSegments.stream()
				.map(segment -> segment.getRange())
				.forEach(range -> includedSegmentRangeInfo.get().addIncludedSegmentRangeInfo(range, range.getStart()));
				
			existingSegments = getSegmentManager().getExistingSegments(new Range(minTime, maxTime), includedSegmentRangeInfo);
			existingSegmentRanges = getSegmentManager().getExistingSegmentRanges(new Range(minTime, maxTime), includedSegmentRangeInfo);
			assertEquals(3, existingSegments.size());
			assertEquals(3, existingSegmentRanges.size());
			
			includedSegmentRangeInfo.get().removeIncludedSegmentRangeInfo(existingSegments.get(0).getRange());
			existingSegments = getSegmentManager().getExistingSegments(new Range(minTime, maxTime), includedSegmentRangeInfo);
			existingSegmentRanges = getSegmentManager().getExistingSegmentRanges(new Range(minTime, maxTime), includedSegmentRangeInfo);
			assertEquals(2, existingSegments.size());
			assertEquals(2, existingSegmentRanges.size());
			
			includedSegmentRangeInfo.get().removeIncludedSegmentRangeInfo(existingSegments.get(0).getRange());
			existingSegments = getSegmentManager().getExistingSegments(new Range(minTime, maxTime), includedSegmentRangeInfo);
			existingSegmentRanges = getSegmentManager().getExistingSegmentRanges(new Range(minTime, maxTime), includedSegmentRangeInfo);
			assertEquals(1, existingSegments.size());
			assertEquals(1, existingSegmentRanges.size());
			
			includedSegmentRangeInfo.get().removeIncludedSegmentRangeInfo(existingSegments.get(0).getRange());
			existingSegments = getSegmentManager().getExistingSegments(new Range(minTime, maxTime), includedSegmentRangeInfo);
			existingSegmentRanges = getSegmentManager().getExistingSegmentRanges(new Range(minTime, maxTime), includedSegmentRangeInfo);
			assertEquals(0, existingSegments.size());
			assertEquals(0, existingSegmentRanges.size());
			
			List<ReadOnlySegment<TestValue>> existingSegments0to0 = getSegmentManager().getExistingSegments(new Range(minTime, minTime), Optional.empty());
			List<Range> existingSegmentRanges0to0 = getSegmentManager().getExistingSegmentRanges(new Range(minTime, minTime), Optional.empty());
			assertEquals(1, existingSegments0to0.size());
			assertEquals(1, existingSegmentRanges0to0.size());
			
			List<ReadOnlySegment<TestValue>> existingSegmentsOutsideRange = getSegmentManager().getExistingSegments(new Range(maxTime * 2, maxTime * 2), Optional.empty());
			List<Range> existingSegmentsRangesOutsideRange = getSegmentManager().getExistingSegmentRanges(new Range(maxTime * 2, maxTime * 2), Optional.empty());
			assertEquals(0, existingSegmentsOutsideRange.size());
			assertEquals(0, existingSegmentsRangesOutsideRange.size());
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
