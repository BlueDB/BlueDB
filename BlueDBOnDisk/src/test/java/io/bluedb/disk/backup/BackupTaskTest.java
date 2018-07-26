package io.bluedb.disk.backup;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.BlueDbOnDisk;
import io.bluedb.disk.BlueDbOnDiskBuilder;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.recovery.PendingChange;
import io.bluedb.disk.recovery.PendingRollup;
import io.bluedb.disk.recovery.Recoverable;
import io.bluedb.disk.segment.Range;

public class BackupTaskTest extends BlueDbDiskTestBase {

	@Test
	public void test_backupToTempDirectory_simple() throws Exception {
		BlueKey key1At1 = createKey(1, 1);
		TestValue value1 = createValue("Anna");
		getTimeCollection().insert(key1At1, value1);
		List<BlueCollectionOnDisk<?>> collectionsToBackup = Arrays.asList(getTimeCollection());

		Path backedUpPath = Files.createTempDirectory(this.getClass().getSimpleName());
		BackupTask backupTask = new BackupTask(db(), backedUpPath);
		backupTask.backupToTempDirectory(collectionsToBackup, backedUpPath);

		BlueDbOnDisk restoredDb = new BlueDbOnDiskBuilder().setPath(backedUpPath).build();
		BlueCollectionOnDisk<TestValue> restoredCollection = (BlueCollectionOnDisk<TestValue>) restoredDb.getCollection(TestValue.class, TimeKey.class, getTimeCollectionName());
		assertTrue(restoredCollection.contains(key1At1));
		assertEquals(value1, restoredCollection.get(key1At1));
		Long restoredMaxLong = restoredCollection.getMaxLongId();
		assertNotNull(restoredMaxLong);
		assertEquals(getTimeCollection().getMaxLongId().longValue(), restoredMaxLong.longValue());
	}

	@Test
	public void test_backupToTempDirectory_change_pending() throws Exception {
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key2At2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
        getTimeCollection().insert(key1At1, value1);
        Recoverable<TestValue> change = PendingChange.createInsert(key2At2, value2, getTimeCollection().getSerializer());
		getRecoveryManager().saveChange(change);

		List<BlueCollectionOnDisk<?>> collectionsToBackup = Arrays.asList(getTimeCollection());
		Path backedUpPath = createTempFolder().toPath();
		BackupTask backupTask = new BackupTask(db(), backedUpPath);
		backupTask.backupToTempDirectory(collectionsToBackup, backedUpPath);

		BlueDbOnDisk restoredDb = new BlueDbOnDiskBuilder().setPath(backedUpPath).build();
        BlueCollectionOnDisk<TestValue> restoredCollection = (BlueCollectionOnDisk<TestValue>) restoredDb.getCollection(TestValue.class, TimeKey.class, getTimeCollectionName());
		assertTrue(restoredCollection.contains(key1At1));
		assertTrue(restoredCollection.contains(key2At2));
		assertEquals(value1, restoredCollection.get(key1At1));
		assertEquals(value2, restoredCollection.get(key2At2));
		Long restoredMaxLong = restoredCollection.getMaxLongId();
		assertNotNull(restoredMaxLong);
		assertEquals(key2At2.getLongIdIfPresent().longValue(), restoredMaxLong.longValue());
	}

	@Test
	public void test_backupToTempDirectory_rollup_pending() throws Exception {
		BlueKey key1At1 = createKey(1, 1);
		TestValue value1 = createValue("Anna");
        getTimeCollection().insert(key1At1, value1);
        Range range = new Range(0, getTimeCollection().getSegmentManager().getSegmentSize() -1);
		Recoverable<TestValue> rollup = new PendingRollup<>(range);
		getRecoveryManager().saveChange(rollup);

		List<BlueCollectionOnDisk<?>> collectionsToBackup = Arrays.asList(getTimeCollection());
		Path backedUpPath = createTempFolder().toPath();
		BackupTask backupTask = new BackupTask(db(), backedUpPath);
		backupTask.backupToTempDirectory(collectionsToBackup, backedUpPath);

        BlueDbOnDisk restoredDb = new BlueDbOnDiskBuilder().setPath(backedUpPath).build();
        BlueCollectionOnDisk<TestValue> restoredCollection = (BlueCollectionOnDisk<TestValue>) restoredDb.getCollection(TestValue.class, TimeKey.class, getTimeCollectionName());
		Path segmentPath = restoredCollection.getSegmentManager().getSegment(1).getPath();
		File[] filesInSegment = segmentPath.toFile().listFiles();
		assertEquals(1, filesInSegment.length);
		assertEquals(1, restoredCollection.query().getList().size());
		assertTrue(restoredCollection.contains(key1At1));
		assertEquals(value1, restoredCollection.get(key1At1));
		Long restoredMaxLong = restoredCollection.getMaxLongId();
		assertNotNull(restoredMaxLong);
		assertEquals(key1At1.getLongIdIfPresent().longValue(), restoredMaxLong.longValue());
	}
}
