package org.bluedb.disk.backup;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.bluedb.TestUtils;
import org.bluedb.api.BlueCollection;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.BlueDbOnDisk;
import org.bluedb.disk.BlueDbOnDiskBuilder;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.TestValue2;
import org.bluedb.disk.TestValueSub;
import org.bluedb.disk.collection.BlueCollectionOnDisk;
import org.bluedb.disk.recovery.PendingChange;
import org.bluedb.disk.recovery.PendingRollup;
import org.bluedb.disk.recovery.Recoverable;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.rollup.RollupTarget;
import org.bluedb.zip.ZipUtils;

public class BackupManagerTest extends BlueDbDiskTestBase {

	@Test
	public void test_backupToTempDirectory_simple() throws Exception {
		BlueKey key1At1 = createKey(1, 1);
		TestValue value1 = createValue("Anna");
		getTimeCollection().insert(key1At1, value1);
		List<BlueCollectionOnDisk<?>> collectionsToBackup = Arrays.asList(getTimeCollection());

		Path backedUpPath = createTempFolder().toPath();
		BackupManager backupTask = db().getBackupManager();
		backupTask.backupToTempDirectory(collectionsToBackup, backedUpPath);

		BlueDbOnDisk restoredDb = new BlueDbOnDiskBuilder().setPath(backedUpPath).build();
		BlueCollectionOnDisk<TestValue> restoredCollection = restoredDb.collectionBuilder(TimeKey.class, TestValue.class).withName(getTimeCollectionName()).build();
		assertTrue(restoredCollection.contains(key1At1));
		assertEquals(value1, restoredCollection.get(key1At1));
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
		BackupManager backupTask = db().getBackupManager();
		backupTask.backupToTempDirectory(collectionsToBackup, backedUpPath);

		BlueDbOnDisk restoredDb = new BlueDbOnDiskBuilder().setPath(backedUpPath).build();
		BlueCollectionOnDisk<TestValue> restoredCollection = restoredDb.collectionBuilder(TimeKey.class, TestValue.class).withName(getTimeCollectionName()).build();
		assertTrue(restoredCollection.contains(key1At1));
		assertTrue(restoredCollection.contains(key2At2));
		assertEquals(value1, restoredCollection.get(key1At1));
		assertEquals(value2, restoredCollection.get(key2At2));
	}

	@Test
	public void test_backupToTempDirectory_rollup_pending() throws Exception {
		BlueKey key1At1 = createKey(1, 1);
		TestValue value1 = createValue("Anna");
        getTimeCollection().insert(key1At1, value1);
        Range range = new Range(0, getTimeCollection().getSegmentManager().getSegmentSize() -1);
		RollupTarget rollupTarget = new RollupTarget(0, range);
		Recoverable<TestValue> rollup = new PendingRollup<>(rollupTarget);
		getRecoveryManager().saveChange(rollup);

		List<BlueCollectionOnDisk<?>> collectionsToBackup = Arrays.asList(getTimeCollection());
		Path backedUpPath = createTempFolder().toPath();
		BackupManager backupTask = db().getBackupManager();
		backupTask.backupToTempDirectory(collectionsToBackup, backedUpPath);

        BlueDbOnDisk restoredDb = new BlueDbOnDiskBuilder().setPath(backedUpPath).build();
		BlueCollectionOnDisk<TestValue> restoredCollection = restoredDb.collectionBuilder(TimeKey.class, TestValue.class).withName(getTimeCollectionName()).build();
		Path segmentPath = restoredCollection.getSegmentManager().getSegment(1).getPath();
		File[] filesInSegment = segmentPath.toFile().listFiles();
		assertEquals(1, filesInSegment.length);
		assertEquals(1, restoredCollection.query().getList().size());
		assertTrue(restoredCollection.contains(key1At1));
		assertEquals(value1, restoredCollection.get(key1At1));
	}

	@Test
	public void test_restore() throws Exception {
		String collectionName = "time_collection";
		BlueDbOnDisk newDb = createTestRestoreDatabase();

		Path zipPath = getbackedUpZipPath();
		if (!zipPath.toFile().exists()) {
			zipPath.getParent().toFile().mkdirs();
			newDb.backup(zipPath);
			fail();
		}

		BlueCollection<TestValue> newCollection =  newDb.getCollection(collectionName, TestValue.class);
		List<TestValue> valuesInNewCollection = newCollection.query().getList();
		
		BlueDbOnDisk restoredDb = getRestoredDatabase(zipPath);
		BlueCollectionOnDisk<TestValue> restoredCollection = restoredDb.collectionBuilder(TimeKey.class, TestValue.class).withName(collectionName).build();
		List<TestValue> valuesInRestoredCollection = restoredCollection.query().getList();
		
		assertEquals(valuesInNewCollection, valuesInRestoredCollection);
	}

	@Test
	public void test_restoreOfOldDomainBackup() throws Exception {
		String collectionName = "time_collection";
		BlueDbOnDisk newDb = createTestRestoreDatabase();

		Path zipPath = TestUtils.getResourcePath("oldDomainBackup.zip");
		if (!zipPath.toFile().exists()) {
			zipPath.getParent().toFile().mkdirs();
			newDb.backup(zipPath);
			fail();
		}

		BlueCollection<TestValue> newCollection =  newDb.getCollection(collectionName, TestValue.class);
		List<TestValue> valuesInNewCollection = newCollection.query().getList();
		
		BlueDbOnDisk restoredDb = getRestoredDatabase(zipPath);
		BlueCollectionOnDisk<TestValue> restoredCollection = restoredDb.collectionBuilder(TimeKey.class, TestValue.class).withName(collectionName).build();
		List<TestValue> valuesInRestoredCollection = restoredCollection.query().getList();
		
		assertEquals(valuesInNewCollection, valuesInRestoredCollection);
	}

	private BlueDbOnDisk getRestoredDatabase(Path zipPath) throws BlueDbException, URISyntaxException, IOException {
		Path restoredDirectory = createTempFolder().toPath();
		ZipUtils.extractFiles(zipPath, restoredDirectory);
		Path restoredDbPath = Paths.get(restoredDirectory.toString(), "bluedb");
		BlueDbOnDisk restoredDb = new BlueDbOnDiskBuilder().setPath(restoredDbPath).build();
		return restoredDb;
	}

	private BlueDbOnDisk createTestRestoreDatabase() throws BlueDbException {
		Path newDbPath = createTempFolder().toPath();
		BlueDbOnDisk newDb = new BlueDbOnDiskBuilder().setPath(newDbPath).build();
		BlueCollectionOnDisk<TestValue> newCollection = newDb.collectionBuilder(TimeKey.class, TestValue.class).withName("time_collection").withRegisteredClasses(TestValue2.class, TestValueSub.class).build();

		TestValue value1 = new TestValue("Anna");
		TestValue value2 = new TestValueSub("Bob");
		TestValue value3 = new TestValue("Carl");

		BlueKey key1 = new TimeFrameKey(1, 1, 4);
		BlueKey key2 = new TimeFrameKey(2, 2, 3);
		BlueKey key3 = new TimeFrameKey(3, 3, 5);
		
		newCollection.insert(key1, value1);
		newCollection.insert(key2, value2);
		newCollection.insert(key3, value3);
		return newDb;
	}

	private Path getbackedUpZipPath() throws URISyntaxException, IOException {
		return TestUtils.getResourcePath("backup.zip");
	}
}
