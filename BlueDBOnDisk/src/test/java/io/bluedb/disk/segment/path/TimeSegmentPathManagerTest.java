package io.bluedb.disk.segment.path;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.segment.Range;

public class TimeSegmentPathManagerTest extends BlueDbDiskTestBase {

	@Test
	public void test_getSegmentRange() {
		Range segmentRangeStartingAtZero = getCollection().getSegmentManager().getSegmentRange(0);
		assertEquals(0, segmentRangeStartingAtZero.getStart());
		assertEquals(getCollection().getSegmentManager().getSegmentSize() - 1, segmentRangeStartingAtZero.getEnd());

		Range maxLongRange = getCollection().getSegmentManager().getSegmentRange(Long.MAX_VALUE);
		assertTrue(maxLongRange.getEnd() > maxLongRange.getStart());
		assertEquals(Long.MAX_VALUE, maxLongRange.getEnd());

		Range minLongRange = getCollection().getSegmentManager().getSegmentRange(Long.MIN_VALUE);
		assertTrue(minLongRange.getEnd() > minLongRange.getStart());
		assertEquals(Long.MIN_VALUE, minLongRange.getStart());
	}

	@Test
	public void test_getSubfoldersInRange() {
		File folder = Paths.get(".", "test_folder").toFile();
		emptyAndDelete(folder);
		folder.mkdir();

		File folder2 = createSubfolder(folder, 2);
		File folder7 = createSubfolder(folder, 7);
		createSegment(folder, 2L, 2L);  // to make sure segment doesn't get included
		
		List<File> empty = Arrays.asList();
		List<File> only2 = Arrays.asList(folder2);
		List<File> both = Arrays.asList(folder2, folder7);
		
		assertEquals(empty, SegmentPathManager.getSubfoldersInRange(folder, 0L, 1L));  // folder above range
		assertEquals(only2, SegmentPathManager.getSubfoldersInRange(folder, 0L, 2L));  // folder at top of range
		assertEquals(only2, SegmentPathManager.getSubfoldersInRange(folder, 0L, 3L));  // folder overlaps top of range
		assertEquals(empty, SegmentPathManager.getSubfoldersInRange(folder, 5L, 6L));  // folder below range
		assertEquals(both, SegmentPathManager.getSubfoldersInRange(folder, 0L, 7L));  //works with multiple files

		emptyAndDelete(folder);
	}

	@Test
	public void test_getExistingSegmentFiles_key() {
		File folder = getCollection().getPath().toFile();
		emptyAndDelete(folder);
		folder.mkdir();

		long startTime = 987654321;
		TimeKey timeKey = new TimeKey(1, startTime);
		TimeFrameKey timeFrameKey = new TimeFrameKey(2, startTime, startTime + TimeSegmentPathManager.SIZE_SEGMENT * 4);

		List<Path> singlePathToAdd = getPathManager().getAllPossibleSegmentPaths(timeKey);
		assertEquals(1, singlePathToAdd.size());
		assertEquals(0, getPathManager().getExistingSegmentFiles(Long.MIN_VALUE, Long.MAX_VALUE).size());
		for (Path path: singlePathToAdd) {
			path.toFile().mkdirs();
		}
		assertEquals(1, getPathManager().getExistingSegmentFiles(Long.MIN_VALUE, Long.MAX_VALUE).size());

		List<Path> fivePaths = getPathManager().getAllPossibleSegmentPaths(timeFrameKey);
		assertEquals(5, fivePaths.size());
		for (Path path: fivePaths) {
			path.toFile().mkdirs();
		}
		assertEquals(5, getPathManager().getExistingSegmentFiles(Long.MIN_VALUE, Long.MAX_VALUE).size());

		emptyAndDelete(folder);
	}

	@Test
	public void test_getAllPossibleSegmentPaths() {
		TimeFrameKey timeFrameKey = new TimeFrameKey(42, 0, TimeSegmentPathManager.SIZE_SEGMENT);
		List<Path> paths = getPathManager().getAllPossibleSegmentPaths(timeFrameKey);
		assertEquals(2, paths.size());
		
		Path secondPath = paths.get(1);
		Path parent = secondPath.getParent();
		Path grandparent = parent.getParent();
		Path greatGrandparent = grandparent.getParent();
		
		String fname = String.valueOf( (TimeSegmentPathManager.SIZE_SEGMENT / TimeSegmentPathManager.SIZE_SEGMENT) );
		String parentName = String.valueOf( (TimeSegmentPathManager.SIZE_SEGMENT / TimeSegmentPathManager.SIZE_FOLDER_BOTTOM) );
		String grandparentName = String.valueOf( (TimeSegmentPathManager.SIZE_SEGMENT / TimeSegmentPathManager.SIZE_FOLDER_MIDDLE) );
		String greatGrandparentName = String.valueOf( (TimeSegmentPathManager.SIZE_SEGMENT / TimeSegmentPathManager.SIZE_FOLDER_TOP) );
		assertEquals(fname, secondPath.getFileName().toString());
		assertEquals(parentName, parent.getFileName().toString());
		assertEquals(grandparentName, grandparent.getFileName().toString());
		assertEquals(greatGrandparentName, greatGrandparent.getFileName().toString());

		BlueKey timeKey = new TimeKey(1, TimeSegmentPathManager.SIZE_SEGMENT);
		List<Path> timePaths = getPathManager().getAllPossibleSegmentPaths(timeKey);
		assertEquals(secondPath, timePaths.get(0));
	}

	@Test
	public void test_getSegmentPath_key() {
		BlueKey key = new TimeKey(5, createTime(4, 3, 2, 1));
		List<Path> paths = getPathManager().getAllPossibleSegmentPaths(key);
		assertEquals(1, paths.size());
		assertEquals(paths.get(0), getPathManager().getSegmentPath(key));
	}

	private SegmentPathManager getPathManager() {
		return getCollection().getSegmentManager().getPathManager();
	}

	public File createSegment(File parentFolder, long low, long high) {
		String segmentName = String.valueOf(low) + "_" + String.valueOf(high);
		File file = Paths.get(parentFolder.toPath().toString(), segmentName).toFile();
		file.mkdir();
		return file;
	}

	private long createTime(long level0, long level1, long level2, long level3) {
		return
				level0 * TimeSegmentPathManager.SIZE_FOLDER_TOP +
				level1 * TimeSegmentPathManager.SIZE_FOLDER_MIDDLE +
				level2 * TimeSegmentPathManager.SIZE_FOLDER_BOTTOM +
				level3 * TimeSegmentPathManager.SIZE_SEGMENT;
	}
}
