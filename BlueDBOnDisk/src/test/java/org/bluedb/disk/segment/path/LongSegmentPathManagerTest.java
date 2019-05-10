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
import org.bluedb.api.keys.LongKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.BlueCollectionOnDisk;
import org.bluedb.disk.segment.Range;
import org.junit.Test;

public class LongSegmentPathManagerTest extends BlueDbDiskTestBase {

	private static String LONG_COLLECTION_NAME = "testing_long";

	BlueCollectionOnDisk<TestValue> longCollection;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		longCollection = db().collectionBuilder(LongKey.class, TestValue.class).withName(LONG_COLLECTION_NAME).build();
	}


	@Test
	public void test_validate_rollup_levels() {
		List<Long> rollupLevels = LongSegmentPathManager.ROLLUP_LEVELS;
		for (int i = 0; i < rollupLevels.size() - 1; i++) {
			assertTrue(rollupLevels.get(i + 1) % rollupLevels.get(i) == 0);
		}
	}

	@Test
	public void test_getSegmentRange() {
		Range segmentRangeStartingAtZero = longCollection.getSegmentManager().getSegmentRange(0);
		assertEquals(0, segmentRangeStartingAtZero.getStart());
		assertEquals(longCollection.getSegmentManager().getSegmentSize() - 1, segmentRangeStartingAtZero.getEnd());

		Range maxLongRange = longCollection.getSegmentManager().getSegmentRange(Long.MAX_VALUE);
		assertTrue(maxLongRange.getEnd() > maxLongRange.getStart());
		assertEquals(Long.MAX_VALUE, maxLongRange.getEnd());

		Range minLongRange = longCollection.getSegmentManager().getSegmentRange(Long.MIN_VALUE);
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
		File folder = longCollection.getPath().toFile();
		emptyAndDelete(folder);
		folder.mkdir();
		LongKey longKey = new LongKey(randomValue());
		List<Path> singlePathToAdd = longCollection.getSegmentManager().getPathManager().getAllPossibleSegmentPaths(longKey);
		assertEquals(1, singlePathToAdd.size());
		assertEquals(0, getPathManager().getExistingSegmentFiles(Long.MIN_VALUE, Long.MAX_VALUE).size());
		for (Path path: singlePathToAdd) {
			path.toFile().mkdirs();
		}
		assertEquals(1, getPathManager().getExistingSegmentFiles(Long.MIN_VALUE, Long.MAX_VALUE).size());
		emptyAndDelete(folder);
	}

	@Test
	public void test_getAllPossibleSegmentPaths() {
		LongKey longKey = new LongKey(randomValue());
		List<Path> paths = getPathManager().getAllPossibleSegmentPaths(longKey);
		assertEquals(1, paths.size());
		
		long groupingNumber = longKey.getGroupingNumber();
		Path path = paths.get(0);
		Path parent = path.getParent();
		Path grandparent = parent.getParent();
		Path greatGrandparent = grandparent.getParent();
		
		List<Long> folderSizes = new ArrayList<>(getPathManager().getFolderSizes());
		Collections.reverse(folderSizes);
		String fname = String.valueOf( (groupingNumber / folderSizes.get(0)) );
		String parentName = String.valueOf( (groupingNumber / folderSizes.get(1)) );
		String grandparentName = String.valueOf( (groupingNumber / folderSizes.get(2)) );
		String greatGrandparentName = String.valueOf( (groupingNumber / folderSizes.get(3)) );
		assertEquals(fname, path.getFileName().toString());
		assertEquals(parentName, parent.getFileName().toString());
		assertEquals(grandparentName, grandparent.getFileName().toString());
		assertEquals(greatGrandparentName, greatGrandparent.getFileName().toString());
	}

	@Test
	public void test_getSegmentPath_key() {
		BlueKey key = new LongKey(randomValue());
		List<Path> paths = getPathManager().getAllPossibleSegmentPaths(key);
		assertEquals(1, paths.size());
		assertEquals(paths.get(0), getPathManager().getSegmentPath(key));
	}

	

	private SegmentPathManager getPathManager() {
		return longCollection.getSegmentManager().getPathManager();
	}

	public File createSegment(File parentFolder, long low, long high) {
		String segmentName = String.valueOf(low) + "_" + String.valueOf(high);
		File file = Paths.get(parentFolder.toPath().toString(), segmentName).toFile();
		file.mkdir();
		return file;
	}

	private long randomValue() {
		return ThreadLocalRandom.current().nextInt();
	}
}
