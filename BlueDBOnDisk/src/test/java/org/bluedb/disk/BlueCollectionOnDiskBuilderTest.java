package org.bluedb.disk;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.bluedb.api.SegmentSize;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.collection.BlueCollectionOnDisk;
import org.bluedb.disk.file.FileUtils;
import org.bluedb.disk.segment.SegmentSizeSettings;
import org.junit.Test;

public class BlueCollectionOnDiskBuilderTest extends BlueDbDiskTestBase {

    @Test
    public void test_differentSegmentSizes() throws Exception {
		BlueCollectionOnDisk<TestValue> hourCollection = (BlueCollectionOnDisk<TestValue>) db.collectionBuilder("hours", TimeKey.class, TestValue.class)
				.withRequestedSegmentSize(SegmentSize.TIME_1_HOUR)
				.build();
		BlueCollectionOnDisk<TestValue> dayCollection = (BlueCollectionOnDisk<TestValue>) db.collectionBuilder("days", TimeKey.class, TestValue.class)
				.withRequestedSegmentSize(SegmentSize.TIME_1_DAY)
				.build();

		TestValue value = new TestValue("Joe");
		BlueKey key = createTimeKey(1558043675317L, value);

		hourCollection.insert(key, value);
		dayCollection.insert(key, value);
		assertEquals(value, hourCollection.get(key));
		assertEquals(value, dayCollection.get(key));

		int dataFoldersForHourly = getFiles(hourCollection).size();
		int dataFoldersForDaily = getFiles(dayCollection).size();
		int expectedDataFoldersForHourly = SegmentSizeSettings.TIME_1_HOUR.getFolderSizes().size();
		int expectedDataFoldersForDaily = SegmentSizeSettings.TIME_1_DAY.getFolderSizes().size();
		assertEquals(expectedDataFoldersForHourly, dataFoldersForHourly);
		assertEquals(expectedDataFoldersForDaily, dataFoldersForDaily);
		assertTrue(dataFoldersForHourly > dataFoldersForDaily);
    }

    
    private static List<File> getFiles(BlueCollectionOnDisk<?> collection) {
    	Path collectionPath = collection.getPath();
    	List<File> results = new ArrayList<>();
    	LinkedList<File> queue = new LinkedList<>();
    	queue.push(collectionPath.toFile());
    	while (!queue.isEmpty()) {
    		List<File> contents = FileUtils.getFolderContents(queue.pop());
    		// add non-metadata directories to results
    		List<File> nonMetadataFolders = contents.stream()
    			.filter((f) -> !f.getName().startsWith("."))
    			.filter((f) -> f.isDirectory())
    			.collect(Collectors.toList());
    		results.addAll(nonMetadataFolders);
    		queue.addAll(nonMetadataFolders);
    	}
    	return results;
    }
    // TODO try opening with the wrong segment size
}
