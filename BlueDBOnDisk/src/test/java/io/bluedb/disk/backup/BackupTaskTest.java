package io.bluedb.disk.backup;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.BlueDbOnDisk;
import io.bluedb.disk.BlueDbOnDiskBuilder;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.recovery.PendingChange;
import io.bluedb.disk.recovery.PendingRollup;
import io.bluedb.disk.recovery.Recoverable;
import io.bluedb.disk.segment.Range;
import io.bluedb.disk.segment.SegmentManager;

public class BackupTaskTest extends BlueDbDiskTestBase {

	@Test
	public void test_backup() {
		try {
			BlueKey key1At1 = createKey(1, 1);
			TestValue value1 = createValue("Anna");
			getCollection().insert(key1At1, value1);

			Path backedUpPath = Files.createTempDirectory(this.getClass().getSimpleName());
			db().backup(backedUpPath);

			BlueDbOnDisk restoredDb = new BlueDbOnDiskBuilder().setPath(backedUpPath).build();
			BlueCollectionOnDisk<TestValue> restoredCollection = (BlueCollectionOnDisk<TestValue>) restoredDb.getCollection(TestValue.class, "testing");
			assertTrue(restoredCollection.contains(key1At1));
			assertEquals(value1, restoredCollection.get(key1At1));
			Long restoredMaxLong = restoredCollection.getMaxLongId();
			assertNotNull(restoredMaxLong);
			assertEquals(getCollection().getMaxLongId().longValue(), restoredMaxLong.longValue());

		} catch (IOException | BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_backup_simple() {
		try {
			BlueKey key1At1 = createKey(1, 1);
			TestValue value1 = createValue("Anna");
			getCollection().insert(key1At1, value1);
			List<BlueCollectionOnDisk<?>> collectionsToBackup = Arrays.asList(getCollection());

			Path backedUpPath = Files.createTempDirectory(this.getClass().getSimpleName());
			BackupTask backupTask = new BackupTask(db(), backedUpPath);
			backupTask.backup(collectionsToBackup);

			BlueDbOnDisk restoredDb = new BlueDbOnDiskBuilder().setPath(backedUpPath).build();
			BlueCollectionOnDisk<TestValue> restoredCollection = (BlueCollectionOnDisk<TestValue>) restoredDb.getCollection(TestValue.class, "testing");
			assertTrue(restoredCollection.contains(key1At1));
			assertEquals(value1, restoredCollection.get(key1At1));
			Long restoredMaxLong = restoredCollection.getMaxLongId();
			assertNotNull(restoredMaxLong);
			assertEquals(getCollection().getMaxLongId().longValue(), restoredMaxLong.longValue());

		} catch (IOException | BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_backup_change_pending() {
		try {
			BlueKey key1At1 = createKey(1, 1);
			BlueKey key2At2 = createKey(2, 2);
			TestValue value1 = createValue("Anna");
			TestValue value2 = createValue("Bob");
			getCollection().insert(key1At1, value1);
			Recoverable<TestValue> change = PendingChange.createInsert(key2At2, value2, getCollection().getSerializer());
			getRecoveryManager().saveChange(change);

			List<BlueCollectionOnDisk<?>> collectionsToBackup = Arrays.asList(getCollection());
			Path backedUpPath = Files.createTempDirectory(this.getClass().getSimpleName());
			BackupTask backupTask = new BackupTask(db(), backedUpPath);
			backupTask.backup(collectionsToBackup);

			BlueDbOnDisk restoredDb = new BlueDbOnDiskBuilder().setPath(backedUpPath).build();
			BlueCollectionOnDisk<TestValue> restoredCollection = (BlueCollectionOnDisk<TestValue>) restoredDb.getCollection(TestValue.class, "testing");
			assertTrue(restoredCollection.contains(key1At1));
			assertTrue(restoredCollection.contains(key2At2));
			assertEquals(value1, restoredCollection.get(key1At1));
			assertEquals(value2, restoredCollection.get(key2At2));
			Long restoredMaxLong = restoredCollection.getMaxLongId();
			assertNotNull(restoredMaxLong);
			assertEquals(key2At2.getLongIdIfPresent().longValue(), restoredMaxLong.longValue());

		} catch (IOException | BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	// TODO if we have more than one change pending, only the last one is grabbed
	@Test
	public void test_backup_rollup_pending() {
		try {
			BlueKey key1At1 = createKey(1, 1);
			TestValue value1 = createValue("Anna");
			getCollection().insert(key1At1, value1);
			Range range = new Range(0, SegmentManager.getSegmentSize() -1);
			Recoverable<TestValue> rollup = new PendingRollup<>(range);
			getRecoveryManager().saveChange(rollup);

			List<BlueCollectionOnDisk<?>> collectionsToBackup = Arrays.asList(getCollection());
			Path backedUpPath = Files.createTempDirectory(this.getClass().getSimpleName());
			BackupTask backupTask = new BackupTask(db(), backedUpPath);
			backupTask.backup(collectionsToBackup);

			BlueDbOnDisk restoredDb = new BlueDbOnDiskBuilder().setPath(backedUpPath).build();
			BlueCollectionOnDisk<TestValue> restoredCollection = (BlueCollectionOnDisk<TestValue>) restoredDb.getCollection(TestValue.class, "testing");
			Path segmentPath = restoredCollection.getSegmentManager().getSegment(1).getPath();
			File[] filesInSegment = segmentPath.toFile().listFiles();
			assertEquals(1, filesInSegment.length);
			assertEquals(1, restoredCollection.query().getList().size());
			assertTrue(restoredCollection.contains(key1At1));
			assertEquals(value1, restoredCollection.get(key1At1));
			Long restoredMaxLong = restoredCollection.getMaxLongId();
			assertNotNull(restoredMaxLong);
			assertEquals(key1At1.getLongIdIfPresent().longValue(), restoredMaxLong.longValue());

		} catch (IOException | BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}
}
