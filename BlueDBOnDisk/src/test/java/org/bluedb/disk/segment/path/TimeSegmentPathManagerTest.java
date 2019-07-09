package org.bluedb.disk.segment.path;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.segment.Range;
import org.junit.Test;

public class TimeSegmentPathManagerTest extends BlueDbDiskTestBase {

	@Test
	public void test_getSegmentRange() {
		Range segmentRangeStartingAtZero = getTimeCollection().getSegmentManager().getSegmentRange(0);
		assertEquals(0, segmentRangeStartingAtZero.getStart());
		assertEquals(getTimeCollection().getSegmentManager().getSegmentSize() - 1, segmentRangeStartingAtZero.getEnd());

		Range maxLongRange = getTimeCollection().getSegmentManager().getSegmentRange(Long.MAX_VALUE);
		assertTrue(maxLongRange.getEnd() > maxLongRange.getStart());
		assertEquals(Long.MAX_VALUE, maxLongRange.getEnd());

		Range minLongRange = getTimeCollection().getSegmentManager().getSegmentRange(Long.MIN_VALUE);
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
		File folder = getTimeCollection().getPath().toFile();
		emptyAndDelete(folder);
		folder.mkdir();

		long startTime = 987654321;
		TimeKey timeKey = new TimeKey(1, startTime);
		TimeFrameKey timeFrameKey = new TimeFrameKey(2, startTime, startTime + getPathManager().getSegmentSize() * 4);

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
		SegmentPathManager pathManager = getPathManager();
		long segmentSize = pathManager.getSegmentSize();
		TimeFrameKey timeFrameKey = new TimeFrameKey(42, 0, segmentSize);
		List<Path> paths = getPathManager().getAllPossibleSegmentPaths(timeFrameKey);
		assertEquals(2, paths.size());
		
		Path secondPath = paths.get(1);
		Path parent = secondPath.getParent();
		Path grandparent = parent.getParent();
		Path greatGrandparent = grandparent.getParent();
		
		List<Long> folderSizes = new ArrayList<>(getPathManager().getFolderSizes());
		Collections.reverse(folderSizes);
		String fname = String.valueOf( (segmentSize / folderSizes.get(0)) );
		String parentName = String.valueOf( (segmentSize / folderSizes.get(1)) );
		String grandparentName = String.valueOf( (segmentSize / folderSizes.get(2)) );
		String greatGrandparentName = String.valueOf( (segmentSize / folderSizes.get(3)) );
		assertEquals(fname, secondPath.getFileName().toString());
		assertEquals(parentName, parent.getFileName().toString());
		assertEquals(grandparentName, grandparent.getFileName().toString());
		assertEquals(greatGrandparentName, greatGrandparent.getFileName().toString());

		BlueKey timeKey = new TimeKey(1, segmentSize);
		List<Path> timePaths = getPathManager().getAllPossibleSegmentPaths(timeKey);
		assertEquals(secondPath, timePaths.get(0));
	}

	@Test
	public void test_getSegmentPath_key() {
		BlueKey key = new TimeKey(5, randomTime());
		List<Path> paths = getPathManager().getAllPossibleSegmentPaths(key);
		assertEquals(1, paths.size());
		assertEquals(paths.get(0), getPathManager().getSegmentPath(key));
	}

	private SegmentPathManager getPathManager() {
		return getTimeCollection().getSegmentManager().getPathManager();
	}

	public File createSegment(File parentFolder, long low, long high) {
		String segmentName = String.valueOf(low) + "_" + String.valueOf(high);
		File file = Paths.get(parentFolder.toPath().toString(), segmentName).toFile();
		file.mkdir();
		return file;
	}

	private long randomTime() {
		long aboutOneHundredYears = 100 * 365 * 24 * 60 * 60 * 1000;
		return ThreadLocalRandom.current().nextLong(aboutOneHundredYears);
	}
}
