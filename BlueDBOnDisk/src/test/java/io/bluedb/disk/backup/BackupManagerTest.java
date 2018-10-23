package io.bluedb.disk.backup;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import io.bluedb.api.BlueCollection;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.BlueDbOnDisk;
import io.bluedb.disk.BlueDbOnDiskBuilder;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.TestValue2;
import io.bluedb.disk.TestValueSub;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.recovery.PendingChange;
import io.bluedb.disk.recovery.PendingRollup;
import io.bluedb.disk.recovery.Recoverable;
import io.bluedb.disk.segment.Range;
import io.bluedb.disk.segment.rollup.RollupTarget;
import io.bluedb.zip.ZipUtils;

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
		BlueCollectionOnDisk<TestValue> restoredCollection = (BlueCollectionOnDisk<TestValue>) restoredDb.initializeCollection(getTimeCollectionName(), TimeKey.class, TestValue.class);
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
        BlueCollectionOnDisk<TestValue> restoredCollection = (BlueCollectionOnDisk<TestValue>) restoredDb.initializeCollection(getTimeCollectionName(), TimeKey.class, TestValue.class);
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
        BlueCollectionOnDisk<TestValue> restoredCollection = (BlueCollectionOnDisk<TestValue>) restoredDb.initializeCollection(getTimeCollectionName(), TimeKey.class, TestValue.class);
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
		@SuppressWarnings("unchecked")
		BlueCollectionOnDisk<TestValue> restoredCollection = (BlueCollectionOnDisk<TestValue>) restoredDb.initializeCollection(collectionName, TimeKey.class, TestValue.class);
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
		@SuppressWarnings("unchecked")
		BlueCollectionOnDisk<TestValue> newCollection = (BlueCollectionOnDisk<TestValue>) newDb.initializeCollection("time_collection", TimeKey.class, TestValue.class, TestValue2.class, TestValueSub.class);

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

	private Path getbackedUpZipPath() throws URISyntaxException {
		URL url = this.getClass().getResource("/backup.zip");
		if (url != null) {
			return Paths.get(url.toURI());
		}
		else {
			String absoluteBinPath = Paths.get(this.getClass().getResource("./").toURI()).toAbsolutePath().toString();
			String absoluteTestPath = absoluteBinPath.replaceFirst("bin.*", "test");
			String filename = "backup.zip";
			return Paths.get(absoluteTestPath, "resources", filename);			
		}
	}
}
