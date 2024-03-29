package org.bluedb.disk.backup;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.bluedb.TestUtils;
import org.bluedb.api.BlueCollection;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.exceptions.UncheckedBlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.ReadWriteDbOnDisk;
import org.bluedb.disk.BlueDbOnDiskBuilder;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.TestValue2;
import org.bluedb.disk.TestValueSub;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.collection.ReadWriteTimeCollectionOnDisk;
import org.bluedb.disk.collection.index.TestRetrievalKeyExtractor;
import org.bluedb.disk.collection.metadata.ReadWriteCollectionMetaData;
import org.bluedb.disk.file.BlueObjectInput;
import org.bluedb.disk.file.ReadWriteFileManager;
import org.bluedb.disk.metadata.BlueFileMetadata;
import org.bluedb.disk.metadata.BlueFileMetadataKey;
import org.bluedb.disk.recovery.PendingChange;
import org.bluedb.disk.recovery.PendingIndexRollup;
import org.bluedb.disk.recovery.PendingRollup;
import org.bluedb.disk.recovery.Recoverable;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadWriteSegment;
import org.bluedb.disk.segment.ReadWriteSegmentManager;
import org.bluedb.disk.segment.rollup.IndexRollupTarget;
import org.bluedb.disk.segment.rollup.RollupTarget;
import org.bluedb.zip.ZipUtils;
import org.junit.Test;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

public class BackupManagerTest extends BlueDbDiskTestBase {

	@Test
	public void test_backupToTempDirectory_simple() throws Exception {
		BlueKey key1At1 = createKey(1, 1);
		TestValue value1 = createValue("Anna");
		getTimeCollection().insert(key1At1, value1);
		List<ReadWriteCollectionOnDisk<?>> collectionsToBackup = Collections.singletonList(getTimeCollection());

		Path backedUpPath = createTempFolder().toPath();
		BackupManager backupTask = db().getBackupManager();
		backupTask.backupToTempDirectory(collectionsToBackup, backedUpPath, Range.createMaxRange(), false);

		ReadWriteDbOnDisk restoredDb = (ReadWriteDbOnDisk) new BlueDbOnDiskBuilder().withPath(backedUpPath).build();
		ReadWriteTimeCollectionOnDisk<TestValue> restoredCollection = (ReadWriteTimeCollectionOnDisk<TestValue>) restoredDb.getTimeCollectionBuilder(getTimeCollectionName(), TimeKey.class, TestValue.class).build();
		assertTrue(restoredCollection.contains(key1At1));
		assertEquals(value1, restoredCollection.get(key1At1));
	}

	@Test
	public void test_backupToTempDirectory_fileManagerThrowsBlueDbException_shouldCascadeExpectedExceptions() throws Exception {
		// Arrange
		BlueKey key1At1 = createKey(1, 1);
		TestValue value1 = createValue("Anna");
		getTimeCollection().insert(key1At1, value1);
		ReadWriteCollectionOnDisk<?> collectionSpy = spy(getTimeCollection());

		ReadWriteCollectionMetaData metadataSpy = spy(collectionSpy.getMetaData());
		ReadWriteFileManager fileManagerMock = mock(ReadWriteFileManager.class);
		doThrow(new BlueDbException("can't make unencrypted copy")).when(fileManagerMock).makeCopy(any(), any());
		doReturn(metadataSpy).when(collectionSpy).getMetaData();
		doReturn(fileManagerMock).when(metadataSpy).getFileManager();
		List<ReadWriteCollectionOnDisk<?>> collectionsToBackup = Collections.singletonList(collectionSpy);

		Path backedUpPath = createTempFolder().toPath();
		BackupManager backupManager = db().getBackupManager();

		// Act
		try {
			backupManager.backupToTempDirectory(collectionsToBackup, backedUpPath, Range.createMaxRange(), false);
			fail("Expected exception was not thrown");
		} catch (BlueDbException ex) {
			// Assert
			assertTrue("Cause of exception was not the expected unchecked exception", ex.getCause() instanceof UncheckedBlueDbException);
		}

	}

	@Test
	public void test_backupToTempDirectory_ExceptionInCopyDataFileStraightOver_shouldSkipFile() throws Exception {
		// Arrange
		IllegalStateException expected = new IllegalStateException("I'm broken!");
		BlueKey key1At1 = createKey(1, 1);
		TestValue value1 = createValue("Anna");
		getTimeCollection().insert(key1At1, value1);

		ReadWriteCollectionOnDisk<?> collectionSpy = spy(getTimeCollection());
		ReadWriteSegmentManager<?> segmentManagerSpy = spy(collectionSpy.getSegmentManager());
		ReadWriteSegment<?> segmentSpy = spy(segmentManagerSpy.getAllExistingSegments().get(0));
		doThrow(expected).when(segmentSpy).getPathFor(anyLong());
		doReturn(segmentManagerSpy).when(collectionSpy).getSegmentManager();
		doReturn(Collections.singletonList(segmentSpy)).when(segmentManagerSpy).getAllExistingSegments();
		List<ReadWriteCollectionOnDisk<?>> collectionsToBackup = Collections.singletonList(collectionSpy);

		Path backedUpPath = createTempFolder().toPath();
		BackupManager backupManager = db().getBackupManager();

		// Act
		backupManager.backupToTempDirectory(collectionsToBackup, backedUpPath, Range.createMaxRange(), false);
	}

	@Test
	public void test_backupToTempDirectory_ExceptionInCopyDataFileAfterFilteringBasedOnTime_shouldSkipFile() throws Exception {
		// Arrange
		IllegalStateException expected = new IllegalStateException("I'm broken!");
		BlueKey key1At1 = createKey(1, 1);
		TestValue value1 = createValue("Anna");
		getTimeCollection().insert(key1At1, value1);

		ReadWriteCollectionOnDisk<?> collectionSpy = spy(getTimeCollection());
		ReadWriteSegmentManager<?> segmentManagerSpy = spy(collectionSpy.getSegmentManager());
		ReadWriteSegment<?> segmentSpy = spy(segmentManagerSpy.getAllExistingSegments().get(0));
		doThrow(expected).when(segmentSpy).getPathFor(anyLong());
		doReturn(segmentManagerSpy).when(collectionSpy).getSegmentManager();
		doReturn(Collections.singletonList(segmentSpy)).when(segmentManagerSpy).getAllExistingSegments();
		List<ReadWriteCollectionOnDisk<?>> collectionsToBackup = Collections.singletonList(collectionSpy);

		Path backedUpPath = createTempFolder().toPath();
		BackupManager backupManager = db().getBackupManager();

		// Act
		backupManager.backupToTempDirectory(collectionsToBackup, backedUpPath, Range.forValueAndRangeSize(1L, 100), false);

	}

	@Test
	public void test_backupToTempDirectory_encryptionEnabledAndUpdatedMetadata_shouldCompleteSuccessfully() throws Exception {
		// Arrange

		BlueKey key1At1 = createKey(1, 1);
		TestValue value1 = createValue("Anna");
		getTimeCollection().insert(key1At1, value1);

		BlueFileMetadata inputMetadata = new BlueFileMetadata();
		inputMetadata.put(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY, "UNIQUE-KEY");
		
		ReadWriteCollectionOnDisk<?> collectionSpy = spy(getTimeCollection());
		ReadWriteSegmentManager<?> segmentManagerSpy = spy(collectionSpy.getSegmentManager());
		ReadWriteSegment<?> segmentSpy = spy(segmentManagerSpy.getAllExistingSegments().get(0));
		BlueObjectInput<?> inputSpy = spy(segmentSpy.getObjectInputFor(1));
		when(inputSpy.getMetadata()).thenReturn(inputMetadata);
		doReturn(segmentManagerSpy).when(collectionSpy).getSegmentManager();
		doReturn(Collections.singletonList(segmentSpy)).when(segmentManagerSpy).getAllExistingSegments();
		doReturn(inputSpy).when(segmentSpy).getObjectInputFor(1);
		List<ReadWriteCollectionOnDisk<?>> collectionsToBackup = Collections.singletonList(collectionSpy);

		Path backedUpPath = createTempFolder().toPath();
		BackupManager backupManager = db().getBackupManager();

		// Act
		backupManager.backupToTempDirectory(collectionsToBackup, backedUpPath, Range.forValueAndRangeSize(1L, 100), false);

		// No assert needed, pass if no exception thrown
	}

	@Test
	public void test_backupToTempDirectory_fileManagerThrowsIOException_shouldCascadeExpectedExceptions() throws Exception {
		// Arrange
		BlueKey key1At1 = createKey(1, 1);
		TestValue value1 = createValue("Anna");
		getTimeCollection().insert(key1At1, value1);

		ReadWriteCollectionOnDisk<?> collectionSpy = spy(getTimeCollection());
		ReadWriteCollectionMetaData metadataSpy = spy(collectionSpy.getMetaData());
		ReadWriteFileManager fileManagerMock = mock(ReadWriteFileManager.class);
		doThrow(new IOException("can't make unencrypted copy")).when(fileManagerMock).makeCopy(any(), any());
		doReturn(metadataSpy).when(collectionSpy).getMetaData();
		doReturn(fileManagerMock).when(metadataSpy).getFileManager();
		List<ReadWriteCollectionOnDisk<?>> collectionsToBackup = Collections.singletonList(collectionSpy);


		Path backedUpPath = createTempFolder().toPath();
		BackupManager backupManager = db().getBackupManager();

		// Act
		try {
			backupManager.backupToTempDirectory(collectionsToBackup, backedUpPath, Range.createMaxRange(), false);
			fail("Expected exception was not thrown");
		} catch (BlueDbException ex) {
			// Assert
			assertTrue("Cause of exception was not the expected unchecked exception", ex.getCause() instanceof UncheckedIOException);
		}

	}

	@Test
	public void test_backupToTempDirectory_change_pending() throws Exception {
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key2At2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		getTimeCollection().insert(key1At1, value1);
		Recoverable<TestValue> change = PendingChange.createInsert(key2At2, value2, getTimeCollection().getSerializer());
		getRecoveryManager().saveNewChange(change);

		List<ReadWriteCollectionOnDisk<?>> collectionsToBackup = Arrays.asList(getTimeCollection());
		Path backedUpPath = createTempFolder().toPath();
		BackupManager backupTask = db().getBackupManager();
		backupTask.backupToTempDirectory(collectionsToBackup, backedUpPath, Range.createMaxRange(), false);

		ReadWriteDbOnDisk restoredDb = (ReadWriteDbOnDisk) new BlueDbOnDiskBuilder().withPath(backedUpPath).build();
		ReadWriteTimeCollectionOnDisk<TestValue> restoredCollection = (ReadWriteTimeCollectionOnDisk<TestValue>) restoredDb.getTimeCollectionBuilder(getTimeCollectionName(), TimeKey.class, TestValue.class).build();
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
		Range range = new Range(0, getTimeCollection().getSegmentManager().getSegmentSize() - 1);
		RollupTarget rollupTarget = new RollupTarget(0, range);
		Recoverable<TestValue> rollup = new PendingRollup<>(rollupTarget);
		getRecoveryManager().saveNewChange(rollup);

		List<ReadWriteCollectionOnDisk<?>> collectionsToBackup = Arrays.asList(getTimeCollection());
		Path backedUpPath = createTempFolder().toPath();
		BackupManager backupTask = db().getBackupManager();
		backupTask.backupToTempDirectory(collectionsToBackup, backedUpPath, Range.createMaxRange(), false);

		ReadWriteDbOnDisk restoredDb = (ReadWriteDbOnDisk) new BlueDbOnDiskBuilder().withPath(backedUpPath).build();
		ReadWriteTimeCollectionOnDisk<TestValue> restoredCollection = (ReadWriteTimeCollectionOnDisk<TestValue>) restoredDb.getTimeCollectionBuilder(getTimeCollectionName(), TimeKey.class, TestValue.class).build();
		Path segmentPath = restoredCollection.getSegmentManager().getSegment(1).getPath();
		File[] filesInSegment = segmentPath.toFile().listFiles();
		assertEquals(1, filesInSegment.length);
		assertEquals(1, restoredCollection.query().getList().size());
		assertTrue(restoredCollection.contains(key1At1));
		assertEquals(value1, restoredCollection.get(key1At1));
	}

	@Test
	public void test_backupToTempDirectory_index_rollup_pending() throws Exception {
		// create the collection and index
		String collectionName = "test_time_collection";
		String indexName = "test_index";
		ReadWriteTimeCollectionOnDisk<TestValue> timeCollection = (ReadWriteTimeCollectionOnDisk<TestValue>) db()
				.getTimeCollectionBuilder(collectionName, TimeKey.class, TestValue.class)
				.build();
		timeCollection.createIndex(indexName, IntegerKey.class, new TestRetrievalKeyExtractor());

		// save a PendingIndexRollup
		Range range = new Range(0, timeCollection.getSegmentManager().getSegmentSize() - 1);
		IndexRollupTarget rollupTarget = new IndexRollupTarget(indexName, 0, range);
		Recoverable<TestValue> indexRollup = new PendingIndexRollup<>(indexName, rollupTarget);
		timeCollection.getRecoveryManager().saveNewChange(indexRollup);

		// backup
		List<ReadWriteCollectionOnDisk<?>> collectionsToBackup = Arrays.asList(timeCollection);
		Path backedUpPath = createTempFolder().toPath();
		BackupManager backupTask = db().getBackupManager();
		backupTask.backupToTempDirectory(collectionsToBackup, backedUpPath, Range.createMaxRange(), false);

		// restore (this failed before we ignored PendingIndexRollup when index doesn't exist.
		ReadWriteDbOnDisk restoredDb = (ReadWriteDbOnDisk) new BlueDbOnDiskBuilder().withPath(backedUpPath).build();
		restoredDb.getTimeCollectionBuilder(collectionName, TimeKey.class, TestValue.class).build();
	}

	@Test
	public void test_restore() throws Exception {
		String collectionName = "time_collection";
		ReadWriteDbOnDisk newDb = createTestRestoreDatabase();

		Path zipPath = getbackedUpZipPath();
		if (!zipPath.toFile().exists()) {
			zipPath.getParent().toFile().mkdirs();
			newDb.backup(zipPath);
			fail();
		}

		BlueCollection<TestValue> newCollection = newDb.getCollection(collectionName, TestValue.class);
		List<TestValue> valuesInNewCollection = newCollection.query().getList();

		ReadWriteDbOnDisk restoredDb = getRestoredDatabase(zipPath);
		ReadWriteTimeCollectionOnDisk<TestValue> restoredCollection = (ReadWriteTimeCollectionOnDisk<TestValue>) restoredDb.getTimeCollectionBuilder(collectionName, TimeKey.class, TestValue.class).build();
		List<TestValue> valuesInRestoredCollection = restoredCollection.query().getList();

		assertEquals(valuesInNewCollection, valuesInRestoredCollection);
	}

	@Test
	public void test_restoreOfOldDomainBackup() throws Exception {
		String collectionName = "time_collection";
		ReadWriteDbOnDisk newDb = createTestRestoreDatabase();

		Path zipPath = TestUtils.getResourcePath("oldDomainBackup.zip");
		if (!zipPath.toFile().exists()) {
			zipPath.getParent().toFile().mkdirs();
			newDb.backup(zipPath);
			fail();
		}

		BlueCollection<TestValue> newCollection = newDb.getCollection(collectionName, TestValue.class);
		List<TestValue> valuesInNewCollection = newCollection.query().getList();

		ReadWriteDbOnDisk restoredDb = getRestoredDatabase(zipPath);
		ReadWriteTimeCollectionOnDisk<TestValue> restoredCollection = (ReadWriteTimeCollectionOnDisk<TestValue>) restoredDb.getTimeCollectionBuilder(collectionName, TimeKey.class, TestValue.class).build();
		List<TestValue> valuesInRestoredCollection = restoredCollection.query().getList();

		assertEquals(valuesInNewCollection, valuesInRestoredCollection);
	}

	private ReadWriteDbOnDisk getRestoredDatabase(Path zipPath) throws BlueDbException, URISyntaxException, IOException {
		Path restoredDirectory = createTempFolder().toPath();
		ZipUtils.extractFiles(zipPath, restoredDirectory);
		Path restoredDbPath = Paths.get(restoredDirectory.toString(), "bluedb");
		ReadWriteDbOnDisk restoredDb = (ReadWriteDbOnDisk) new BlueDbOnDiskBuilder().withPath(restoredDbPath).build();
		return restoredDb;
	}

	private ReadWriteDbOnDisk createTestRestoreDatabase() throws BlueDbException {
		Path newDbPath = createTempFolder().toPath();
		ReadWriteDbOnDisk newDb = (ReadWriteDbOnDisk) new BlueDbOnDiskBuilder().withPath(newDbPath).build();
		ReadWriteTimeCollectionOnDisk<TestValue> newCollection = (ReadWriteTimeCollectionOnDisk<TestValue>) newDb.getTimeCollectionBuilder("time_collection", TimeKey.class, TestValue.class)
				.withOptimizedClasses(Arrays.asList(TestValue2.class, TestValueSub.class))
				.build();

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
