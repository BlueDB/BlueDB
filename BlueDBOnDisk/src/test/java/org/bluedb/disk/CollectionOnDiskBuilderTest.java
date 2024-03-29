package org.bluedb.disk;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.bluedb.api.BlueCollectionVersion;
import org.bluedb.api.SegmentSize;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.file.FileUtils;
import org.bluedb.disk.segment.SegmentSizeSetting;
import org.junit.Test;

public class CollectionOnDiskBuilderTest extends BlueDbDiskTestBase {
	
	@Test
	public void test_differentVersions() throws BlueDbException {
		ReadWriteCollectionOnDisk<TestValue> version1Collection = (ReadWriteCollectionOnDisk<TestValue>) db.getCollectionBuilder("version-1-a", TimeKey.class, TestValue.class)
				.withCollectionVersion(BlueCollectionVersion.VERSION_1)
				.build();
		
		ReadWriteCollectionOnDisk<TestValue> version2Collection = (ReadWriteCollectionOnDisk<TestValue>) db.getCollectionBuilder("version-2-a", TimeKey.class, TestValue.class)
				.withCollectionVersion(BlueCollectionVersion.VERSION_2)
				.build();
		
		assertEquals(BlueCollectionVersion.VERSION_1, version1Collection.getVersion());
		assertEquals(BlueCollectionVersion.VERSION_2, version2Collection.getVersion());
		
		version1Collection = (ReadWriteCollectionOnDisk<TestValue>) db.getTimeCollectionBuilder("version-1-b", TimeKey.class, TestValue.class)
				.withCollectionVersion(BlueCollectionVersion.VERSION_1)
				.build();
		
		version2Collection = (ReadWriteCollectionOnDisk<TestValue>) db.getTimeCollectionBuilder("version-2-b", TimeKey.class, TestValue.class)
				.withCollectionVersion(BlueCollectionVersion.VERSION_2)
				.build();
		
		assertEquals(BlueCollectionVersion.VERSION_1, version1Collection.getVersion());
		assertEquals(BlueCollectionVersion.VERSION_2, version2Collection.getVersion());
	}

    @Test
    public void test_differentSegmentSizes() throws Exception {
		ReadWriteCollectionOnDisk<TestValue> hourCollection = (ReadWriteCollectionOnDisk<TestValue>) db.getCollectionBuilder("hours", TimeKey.class, TestValue.class)
				.withSegmentSize(SegmentSize.TIME_1_HOUR)
				.build();
		ReadWriteCollectionOnDisk<TestValue> dayCollection = (ReadWriteCollectionOnDisk<TestValue>) db.getCollectionBuilder("days", TimeKey.class, TestValue.class)
				.withSegmentSize(SegmentSize.TIME_1_DAY)
				.build();

		TestValue value = new TestValue("Joe");
		BlueKey key = createTimeKey(1558043675317L, value);

		hourCollection.insert(key, value);
		dayCollection.insert(key, value);
		assertEquals(value, hourCollection.get(key));
		assertEquals(value, dayCollection.get(key));

		int dataFoldersForHourly = getFiles(hourCollection).size();
		int dataFoldersForDaily = getFiles(dayCollection).size();
		int expectedDataFoldersForHourly = SegmentSizeSetting.TIME_1_HOUR.getFolderSizes().size();
		int expectedDataFoldersForDaily = SegmentSizeSetting.TIME_1_DAY.getFolderSizes().size();
		assertEquals(expectedDataFoldersForHourly, dataFoldersForHourly);
		assertEquals(expectedDataFoldersForDaily, dataFoldersForDaily);
		assertTrue(dataFoldersForHourly > dataFoldersForDaily);
    }

    @Test
    public void test_reopeningSegmentWithDifferentSizes() throws Exception {
		db.getCollectionBuilder("hours", TimeKey.class, TestValue.class)
				.withSegmentSize(SegmentSize.TIME_1_HOUR)
				.build();
		db.shutdown();
		db.awaitTermination(1, TimeUnit.MINUTES);
		db = (ReadWriteDbOnDisk) new BlueDbOnDiskBuilder().withPath(dbPath).build();  // reopen
		
		ReadWriteCollectionOnDisk<TestValue> hourCollectionReopenedAsDaily = (ReadWriteCollectionOnDisk<TestValue>) db.getCollectionBuilder("hours", TimeKey.class, TestValue.class)
				.withSegmentSize(SegmentSize.TIME_1_DAY)
				.build();

		TestValue value = new TestValue("Joe");
		BlueKey key = createTimeKey(1558043675317L, value);

		hourCollectionReopenedAsDaily.insert(key, value);
		assertEquals(value, hourCollectionReopenedAsDaily.get(key));

		int dataFoldersForHourly = getFiles(hourCollectionReopenedAsDaily).size();
		int expectedDataFoldersForHourly = SegmentSizeSetting.TIME_1_HOUR.getFolderSizes().size();
		assertEquals(expectedDataFoldersForHourly, dataFoldersForHourly);
    }
    
    private static List<File> getFiles(ReadWriteCollectionOnDisk<?> collection) {
    	Path collectionPath = collection.getPath();
    	List<File> results = new ArrayList<>();
    	LinkedList<File> queue = new LinkedList<>();
    	queue.push(collectionPath.toFile());
    	while (!queue.isEmpty()) {
    		List<File> subFolders = FileUtils.getSubFolders(queue.pop());
    		// add non-metadata directories to results
    		List<File> nonMetadataFolders = subFolders.stream()
    			.filter((f) -> !f.getName().startsWith("."))
    			.collect(Collectors.toList());
    		results.addAll(nonMetadataFolders);
    		queue.addAll(nonMetadataFolders);
    	}
    	return results;
    }
}
