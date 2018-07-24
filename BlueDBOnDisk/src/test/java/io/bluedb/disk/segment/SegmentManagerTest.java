package io.bluedb.disk.segment;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.segment.path.TimeSegmentPathManager;

public class SegmentManagerTest extends BlueDbDiskTestBase {

	@Test
	public void test_getAllSegments() {
		BlueKey timeFrameKey = new TimeFrameKey(1, 0, TimeSegmentPathManager.SIZE_SEGMENT);
		List<Path> timeFramePaths = getSegmentManager().getPathManager().getAllPossibleSegmentPaths(timeFrameKey);
		List<Segment<TestValue>> timeFrameSegments = getSegmentManager().getAllSegments(timeFrameKey);
		List<Path> timeFrameSegmentPaths = timeFrameSegments.stream().map((s) -> s.getPath()).collect(Collectors.toList());

		assertEquals(timeFramePaths, timeFrameSegmentPaths);
		
		BlueKey timeKey = new TimeKey(1, TimeSegmentPathManager.SIZE_SEGMENT);
		List<Path> timePaths = getSegmentManager().getPathManager().getAllPossibleSegmentPaths(timeKey);
		List<Segment<TestValue>> timeSegments = getSegmentManager().getAllSegments(timeKey);
		List<Path> segmentPaths = timeSegments.stream().map((s) -> s.getPath()).collect(Collectors.toList());
		assertEquals(timePaths, segmentPaths);
	}

	@Test
	public void test_getExistingSegments() {
		emptyAndDelete(getCollection().getPath().toFile());
		long minTime = 0;
		long maxTime = TimeSegmentPathManager.SIZE_SEGMENT * 2;
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
		BlueKey timeFrameKey = new TimeFrameKey(1, TimeSegmentPathManager.SIZE_SEGMENT, TimeSegmentPathManager.SIZE_SEGMENT * 2);
		List<Segment<TestValue>> timeFrameSegments = getSegmentManager().getAllSegments(timeFrameKey);
		assertEquals(2, timeFrameSegments.size());
		assertEquals(timeFrameSegments.get(0), getSegmentManager().getFirstSegment(timeFrameKey));
	}

	@Test
	public void test_toSegment() {
		BlueKey key = new TimeKey(5, createTime(4, 3, 2, 1));
		Path path = getSegmentManager().getPathManager().getSegmentPath(key);
		Segment<TestValue> segment = getSegmentManager().toSegment(path);
		assertEquals(path, segment.getPath());
	}



	public File createSegment(File parentFolder, long low, long high) {
		String segmentName = String.valueOf(low) + "_" + String.valueOf(high);
		File file = Paths.get(parentFolder.toPath().toString(), segmentName).toFile();
		file.mkdir();
		return file;
	}

	private SegmentManager<TestValue> getSegmentManager() {
		return getCollection().getSegmentManager();
	}

	private long createTime(long level0, long level1, long level2, long level3) {
		return
				level0 * TimeSegmentPathManager.SIZE_FOLDER_TOP +
				level1 * TimeSegmentPathManager.SIZE_FOLDER_MIDDLE +
				level2 * TimeSegmentPathManager.SIZE_FOLDER_BOTTOM +
				level3 * TimeSegmentPathManager.SIZE_SEGMENT;
	}
}
