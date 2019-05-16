package org.bluedb.disk.segment;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.HashGroupedKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.segment.path.SegmentPathManager;
import org.junit.Test;

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
		emptyAndDelete(getTimeCollection().getPath().toFile());
		long minTime = 0;
		long segmentSize = getSegmentManager().getSegmentSize();
		long maxTime = segmentSize * 2;
		BlueKey timeFrameKey = new TimeFrameKey(1, minTime, maxTime);  // should barely span 3 segments
		TestValue value = new TestValue("Bob", 0);
		try {
			List<Segment<TestValue>> existingSegments = getSegmentManager().getExistingSegments(new Range(minTime, maxTime));
			assertEquals(0, existingSegments.size());
			getTimeCollection().insert(timeFrameKey, value);
			existingSegments = getSegmentManager().getExistingSegments(new Range(minTime, maxTime));
			assertEquals(3, existingSegments.size());
			List<Segment<TestValue>> existingSegments0to0 = getSegmentManager().getExistingSegments(new Range(minTime, minTime));
			assertEquals(1, existingSegments0to0.size());
			List<Segment<TestValue>> existingSegmentsOutsideRange = getSegmentManager().getExistingSegments(new Range(maxTime * 2, maxTime * 2));
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

	@Test
	public void test_createSegmentPathManager() throws BlueDbException {
		Path collectionPath = getTimeCollection().getPath();
		SegmentPathManager timePathManager = SegmentManager.createSegmentPathManager(collectionPath, TimeKey.class);
		SegmentPathManager valuePathManager = SegmentManager.createSegmentPathManager(collectionPath, TimeKey.class);
		assertNotNull(timePathManager);
		assertNotNull(valuePathManager);

		try {
			SegmentManager.createSegmentPathManager(collectionPath, BlueKey.class);
		} catch (UnsupportedOperationException | BlueDbException e) {}
		try {
			SegmentManager.createSegmentPathManager(collectionPath, null);
		} catch (UnsupportedOperationException | NullPointerException e) {
		}
	}

	List<Long> foldersLong = Arrays.asList(72057594037927936L, 562949953421312L, 2199023255552L, 4294967296L, 8388608L, 16384L, 64L);
	List<Long> foldersTime = Arrays.asList(31104000000L, 2592000000L, 86400000L, 3600000L);
	List<Long> foldersInt = Arrays.asList(67108864L, 1048576L, 16384L, 256L);
	List<Long> foldersHash = Arrays.asList(4294967296L, 67108864L, 524288L);

	@Test
	public void test_createSegmentPathManager_sizes() throws BlueDbException {
		Path collectionPath = getTimeCollection().getPath();
		assertEquals(foldersLong, SegmentManager.createSegmentPathManager(collectionPath, LongKey.class).getFolderSizes());
		assertEquals(foldersTime, SegmentManager.createSegmentPathManager(collectionPath, TimeKey.class).getFolderSizes());
		assertEquals(foldersInt, SegmentManager.createSegmentPathManager(collectionPath, IntegerKey.class).getFolderSizes());
		assertEquals(foldersHash, SegmentManager.createSegmentPathManager(collectionPath, HashGroupedKey.class).getFolderSizes());
	}

	private SegmentManager<TestValue> getSegmentManager() {
		return getTimeCollection().getSegmentManager();
	}


	private long randomTime() {
		long aboutOneHundredYears = 100 * 365 * 24 * 60 * 60 * 1000;
		return ThreadLocalRandom.current().nextLong(aboutOneHundredYears);
	}
}
