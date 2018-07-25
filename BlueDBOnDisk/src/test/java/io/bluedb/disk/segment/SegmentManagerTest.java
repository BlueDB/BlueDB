package io.bluedb.disk.segment;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.junit.Test;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.TestValue;

public class SegmentManagerTest extends BlueDbDiskTestBase {

	@Test
	public void test_getAllSegments() {
		long segmentSize = getSegmentManager().getSegmentSize();
		BlueKey timeFrameKey = new TimeFrameKey(1, 0, segmentSize);
		List<Path> timeFramePaths = getSegmentManager().getPathManager().getAllPossibleSegmentPaths(timeFrameKey);
		List<Segment<TestValue>> timeFrameSegments = getSegmentManager().getAllSegments(timeFrameKey);
		List<Path> timeFrameSegmentPaths = timeFrameSegments.stream().map((s) -> s.getPath()).collect(Collectors.toList());

		assertEquals(timeFramePaths, timeFrameSegmentPaths);
		
		BlueKey timeKey = new TimeKey(1, segmentSize);
		List<Path> timePaths = getSegmentManager().getPathManager().getAllPossibleSegmentPaths(timeKey);
		List<Segment<TestValue>> timeSegments = getSegmentManager().getAllSegments(timeKey);
		List<Path> segmentPaths = timeSegments.stream().map((s) -> s.getPath()).collect(Collectors.toList());
		assertEquals(timePaths, segmentPaths);
	}

	@Test
	public void test_getExistingSegments() {
		emptyAndDelete(getCollection().getPath().toFile());
		long minTime = 0;
		long segmentSize = getSegmentManager().getSegmentSize();
		long maxTime = segmentSize * 2;
		BlueKey timeFrameKey = new TimeFrameKey(1, minTime, maxTime);  // should barely span 3 segments
		TestValue value = new TestValue("Bob", 0);
		try {
			List<Segment<TestValue>> existingSegments = getSegmentManager().getExistingSegments(minTime, maxTime);
			assertEquals(0, existingSegments.size());
			getCollection().insert(timeFrameKey, value);
			existingSegments = getSegmentManager().getExistingSegments(minTime, maxTime);
			assertEquals(3, existingSegments.size());
			List<Segment<TestValue>> existingSegments0to0 = getSegmentManager().getExistingSegments(minTime, minTime);
			assertEquals(1, existingSegments0to0.size());
			List<Segment<TestValue>> existingSegmentsOutsideRange = getSegmentManager().getExistingSegments(maxTime * 2, maxTime * 2);
			assertEquals(0, existingSegmentsOutsideRange.size());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		emptyAndDelete(getCollection().getPath().toFile());
	}

	@Test
	public void test_getFirstSegment() {
		long segmentSize = getSegmentManager().getSegmentSize();
		BlueKey timeFrameKey = new TimeFrameKey(1, segmentSize, segmentSize * 2);
		List<Segment<TestValue>> timeFrameSegments = getSegmentManager().getAllSegments(timeFrameKey);
		assertEquals(2, timeFrameSegments.size());
		assertEquals(timeFrameSegments.get(0), getSegmentManager().getFirstSegment(timeFrameKey));
	}

	@Test
	public void test_toSegment() {
		BlueKey key = new TimeKey(5, randomTime());
		Path path = getSegmentManager().getPathManager().getSegmentPath(key);
		Segment<TestValue> segment = getSegmentManager().toSegment(path);
		assertEquals(path, segment.getPath());
	}



	private SegmentManager<TestValue> getSegmentManager() {
		return getCollection().getSegmentManager();
	}


	private long randomTime() {
		long aboutOneHundredYears = 100 * 365 * 24 * 60 * 60 * 1000;
		return ThreadLocalRandom.current().nextLong(aboutOneHundredYears);
	}
}
