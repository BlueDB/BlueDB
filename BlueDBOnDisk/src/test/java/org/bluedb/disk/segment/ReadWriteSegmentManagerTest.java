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
import org.bluedb.disk.TestValue;
import org.junit.Test;

public class ReadWriteSegmentManagerTest extends BlueDbDiskTestBase {

	@Test
	public void test_getAllSegments() {
		long segmentSize = getSegmentManager().getSegmentSize();
		BlueKey timeFrameKey = new TimeFrameKey(1, 0, segmentSize);
		List<Path> timeFramePaths = getSegmentManager().getPathManager().getAllPossibleSegmentPaths(timeFrameKey);
		List<ReadWriteSegment<TestValue>> timeFrameSegments = getSegmentManager().getAllSegments(timeFrameKey);
		List<Path> timeFrameSegmentPaths = timeFrameSegments.stream().map((s) -> s.getPath()).collect(Collectors.toList());

		assertEquals(timeFramePaths, timeFrameSegmentPaths);
		
		BlueKey timeKey = new TimeKey(1, segmentSize);
		List<Path> timePaths = getSegmentManager().getPathManager().getAllPossibleSegmentPaths(timeKey);
		List<ReadWriteSegment<TestValue>> timeSegments = getSegmentManager().getAllSegments(timeKey);
		List<Path> segmentPaths = timeSegments.stream().map((s) -> s.getPath()).collect(Collectors.toList());
		assertEquals(timePaths, segmentPaths);
	}

	@Test
	public void test_getExistingSegments() {
		emptyAndDelete(getTimeCollection().getPath().toFile());
		long minTime = 0;
		long segmentSize = getSegmentManager().getSegmentSize();
		long maxTime = segmentSize * 2;
		BlueKey timeFrameKey = new TimeFrameKey(1, minTime, maxTime);  // should barely span 3 segments
		TestValue value = new TestValue("Bob", 0);
		try {
			List<ReadWriteSegment<TestValue>> existingSegments = getSegmentManager().getExistingSegments(new Range(minTime, maxTime), Optional.empty());
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
			
			List<ReadWriteSegment<TestValue>> existingSegments0to0 = getSegmentManager().getExistingSegments(new Range(minTime, minTime), Optional.empty());
			assertEquals(1, existingSegments0to0.size());
			
			List<ReadWriteSegment<TestValue>> existingSegmentsOutsideRange = getSegmentManager().getExistingSegments(new Range(maxTime * 2, maxTime * 2), Optional.empty());
			assertEquals(0, existingSegmentsOutsideRange.size());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		emptyAndDelete(getTimeCollection().getPath().toFile());
	}

	@Test
	public void test_getFirstSegment() {
		long segmentSize = getSegmentManager().getSegmentSize();
		BlueKey timeFrameKey = new TimeFrameKey(1, segmentSize, segmentSize * 2);
		List<ReadWriteSegment<TestValue>> timeFrameSegments = getSegmentManager().getAllSegments(timeFrameKey);
		assertEquals(2, timeFrameSegments.size());
		assertEquals(timeFrameSegments.get(0), getSegmentManager().getFirstSegment(timeFrameKey));
	}

	@Test
	public void test_toSegment() {
		BlueKey key = new TimeKey(5, randomTime());
		Path path = getSegmentManager().getPathManager().getSegmentPath(key);
		ReadWriteSegment<TestValue> segment = getSegmentManager().toSegment(path);
		assertEquals(path, segment.getPath());
	}

	@Test
	public void test_toRange() {
		long segmentSize = getTimeCollection().getSegmentManager().getSegmentSize();
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

	private ReadWriteSegmentManager<TestValue> getSegmentManager() {
		return getTimeCollection().getSegmentManager();
	}


	private long randomTime() {
		long aboutOneHundredYears = 100 * 365 * 24 * 60 * 60 * 1000;
		return ThreadLocalRandom.current().nextLong(aboutOneHundredYears);
	}
}
