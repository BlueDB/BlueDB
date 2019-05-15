package org.bluedb.disk.segment.path;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.bluedb.disk.segment.SegmentManager;
import org.junit.Test;

public class SegmentPathManagerTest {


	@Test
	public void test_lock_rollup_levels() {
		assertEquals(Arrays.asList(1L, 64L), SegmentManager.DEFAULT_ROLLUP_LEVELS_LONG);
		assertEquals(Arrays.asList(1L, 6_000L, 3_600_000L), SegmentManager.DEFAULT_ROLLUP_LEVELS_TIME);
		assertEquals(Arrays.asList(1L, 256L), SegmentManager.DEFAULT_ROLLUP_LEVELS_INTEGER);
		assertEquals(Arrays.asList(1L, 524288L), SegmentManager.DEFAULT_ROLLUP_LEVELS_HASH);
	}

	List<Long> foldersLong = Arrays.asList(72057594037927936L, 562949953421312L, 2199023255552L, 4294967296L, 8388608L, 16384L, 64L);
	List<Long> foldersTime = Arrays.asList(31104000000L, 2592000000L, 86400000L, 3600000L);
	List<Long> foldersInt = Arrays.asList(67108864L, 1048576L, 16384L, 256L);
	List<Long> foldersHash = Arrays.asList(4294967296L, 67108864L, 524288L);

	@Test
	public void test_lock_folder_levels() {
		assertEquals(foldersLong, SegmentManager.DEFAULT_SIZE_FOLDERS_LONG);
		assertEquals(foldersTime, SegmentManager.DEFAULT_SIZE_FOLDERS_TIME);
		assertEquals(foldersInt, SegmentManager.DEFAULT_SIZE_FOLDERS_INTEGER);
		assertEquals(foldersHash, SegmentManager.DEFAULT_SIZE_FOLDERS_HASH);
	}
}
