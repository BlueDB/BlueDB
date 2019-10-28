package org.bluedb.disk.recovery;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import org.bluedb.api.Updater;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.file.FileUtils;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;

public class RecoveryManagerTest extends BlueDbDiskTestBase {

	BlueSerializer serializer;

	@Override
	public void setUp() throws Exception {
		super.setUp();
		serializer = new ThreadLocalFstSerializer(new Class[] {});
	}

	@Test
	public void test_getPendingFileName() throws Exception {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		BlueSerializer serializer = new ThreadLocalFstSerializer(new Class[] {});
		PendingChange<TestValue> change = PendingChange.createInsert(key, value, serializer);
		String fileName1 = RecoveryManager.getPendingFileName(change);

		Thread.sleep(1);

		PendingChange<TestValue> change2 = PendingChange.createInsert(key, value, serializer);
		String fileName2 = RecoveryManager.getPendingFileName(change2);
		assertTrue(fileName1.compareTo(fileName2) < 0);
	}

	@Test
	public void test_getCompletedFileName() throws Exception {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		BlueSerializer serializer = new ThreadLocalFstSerializer(new Class[] {});
		PendingChange<TestValue> change = PendingChange.createInsert(key, value, serializer);
		String pendingFileName = RecoveryManager.getPendingFileName(change);
		String completedFileName = RecoveryManager.getCompletedFileName(change);
		File pendingFile = Paths.get(pendingFileName).toFile();
		File completedFile = Paths.get(completedFileName).toFile();
		TimeStampedFile pendingTimeStampedFile = new TimeStampedFile(pendingFile);
		TimeStampedFile completedTimeStampedFile = new TimeStampedFile(completedFile);
		assertEquals(0, pendingTimeStampedFile.compareTo(completedTimeStampedFile));
	}

	@Test
	public void test_saveChange() throws Exception {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		Recoverable<TestValue> change = PendingChange.createInsert(key, value, serializer);

		List<Recoverable<TestValue>> changes = getRecoveryManager().getPendingChanges();
		assertEquals(0, changes.size());
		getRecoveryManager().saveChange(change);
		changes = getRecoveryManager().getPendingChanges();
		PendingChange<TestValue> savedChange = (PendingChange<TestValue>) changes.get(0);
		assertEquals(1, changes.size());
		assertEquals(value, savedChange.getNewValue());
	}

	@Test
	public void test_markComplete() throws Exception {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		Recoverable<TestValue> change = PendingChange.createInsert(key, value, serializer);

		getRecoveryManager().saveChange(change);
		List<Recoverable<TestValue>> changes = getRecoveryManager().getPendingChanges();
		assertEquals(1, changes.size());
		getRecoveryManager().markComplete(change);
		changes = getRecoveryManager().getPendingChanges();
		assertEquals(0, changes.size());
	}

	@Test
	public void test_getPendingChanges() throws Exception {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		PendingChange<TestValue> change = PendingChange.createInsert(key, value, serializer);

		List<Recoverable<TestValue>> changes = getRecoveryManager().getPendingChanges();
		assertEquals(0, changes.size());
		getRecoveryManager().saveChange(change);
		changes = getRecoveryManager().getPendingChanges();
		assertEquals(1, changes.size());
		getRecoveryManager().markComplete(change);
		changes = getRecoveryManager().getPendingChanges();
		assertEquals(0, changes.size());
	}

	@Test
	public void test_getPendingChangeFiles() throws Exception {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		PendingChange<TestValue> change = PendingChange.createInsert(key, value, serializer);

		List<File> changes = getRecoveryManager().getPendingChangeFiles();
		assertEquals(0, changes.size());

		getRecoveryManager().saveChange(change);
		changes = getRecoveryManager().getPendingChangeFiles();
		assertEquals(1, changes.size());
		
		File changeFile = changes.get(0);
		File tempFile = FileUtils.createTempFilePath(changeFile.toPath()).toFile();
		tempFile.createNewFile();
		Path historyFolderPath = changeFile.getParentFile().toPath();
		List<File> allFilesInChangeFolder = Arrays.asList(historyFolderPath.toFile().listFiles()); // [pending change temp file, pending change file]
		assertEquals(2, allFilesInChangeFolder.size());
		changes = getRecoveryManager().getPendingChangeFiles();
		assertEquals(1, changes.size());  // ignores temp file

		getRecoveryManager().markComplete(change);
		changes = getRecoveryManager().getPendingChangeFiles();
		assertEquals(0, changes.size());

		List<File> completedChanges = getRecoveryManager().getCompletedChangeFiles();
		assertEquals(1, completedChanges.size());

		File completedChangeFile = completedChanges.get(0);
		File tempCompletedFile = FileUtils.createTempFilePath(completedChangeFile.toPath()).toFile();
		tempCompletedFile.createNewFile();
		allFilesInChangeFolder = Arrays.asList(historyFolderPath.toFile().listFiles());
		assertEquals(3, allFilesInChangeFolder.size());  // [pending change temp file, complete change temp file, complete change file]
		completedChanges = getRecoveryManager().getCompletedChangeFiles();
		assertEquals(1, completedChanges.size());
	}

	@Test
	public void test_getChangeHistory() throws Exception {
		long thirtyMinutesAgo = System.currentTimeMillis() - 30 * 60 * 1000;
		long sixtyMinutesAgo = System.currentTimeMillis() - 60 * 60 * 1000;
		long ninetyMinutesAgo = System.currentTimeMillis() - 90 * 60 * 1000;
		Recoverable<TestValue> change30 = createRecoverable(thirtyMinutesAgo);
		Recoverable<TestValue> change60 = createRecoverable(sixtyMinutesAgo);
		Recoverable<TestValue> change90 = createRecoverable(ninetyMinutesAgo);
		assertEquals(thirtyMinutesAgo, change30.getTimeCreated());
		assertEquals(sixtyMinutesAgo, change60.getTimeCreated());
		assertEquals(ninetyMinutesAgo, change90.getTimeCreated());
		getRecoveryManager().getChangeHistoryCleaner().setWaitBetweenCleanups(100_000);  // to prevent automatic cleanup
		List<File> changesInitial = getRecoveryManager().getChangeHistory(Long.MIN_VALUE, Long.MAX_VALUE);
		getRecoveryManager().saveChange(change30);
		getRecoveryManager().saveChange(change60);
		getRecoveryManager().saveChange(change90);
		List<File> changesAll = getRecoveryManager().getChangeHistory(Long.MIN_VALUE, Long.MAX_VALUE);
		List<File> changes30to60 = getRecoveryManager().getChangeHistory(sixtyMinutesAgo, thirtyMinutesAgo);
		List<File> changes30to30 = getRecoveryManager().getChangeHistory(thirtyMinutesAgo, thirtyMinutesAgo);
		List<File> changesJustBefore30to30 = getRecoveryManager().getChangeHistory(thirtyMinutesAgo-1, thirtyMinutesAgo);
		List<File> changes30to90 = getRecoveryManager().getChangeHistory(ninetyMinutesAgo, thirtyMinutesAgo);
		List<File> changes0to0 = getRecoveryManager().getChangeHistory(0, 0);
		assertEquals(0, changesInitial.size());
		assertEquals(3, changesAll.size());
		assertEquals(3, changes30to60.size()); // includes change before time period that may be partly completed at backup
		assertEquals(2, changes30to30.size()); // includes change before time period that may be partly completed at backup
		assertEquals(2, changesJustBefore30to30.size()); // includes change before time period that may be partly completed at backup
		assertEquals(3, changes30to90.size());
		assertEquals(0, changes0to0.size());
	}

	@Test
	public void test_recover_pendingInsert() throws Exception {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");

		PendingChange<TestValue> change = PendingChange.createInsert(key, value, serializer);
		getRecoveryManager().saveChange(change);
		List<TestValue> allValues = getTimeCollection().query().getList();
		assertEquals(0, allValues.size());

		getRecoveryManager().recover();
		allValues = getTimeCollection().query().getList();
		assertEquals(1, allValues.size());
		assertEquals(value, allValues.get(0));
	}

	@Test
	public void test_recover_pendingBatchUpdate() throws Exception {
		BlueKey key1at2 = createKey(1, 2);
		BlueKey key2at3 = createKey(2, 3);
		TestValue value1 = createValue("Joe");
		TestValue value2 = createValue("Bob");
		List<IndividualChange<TestValue>> sortedChanges = Arrays.asList(
				IndividualChange.createInsertChange(key1at2, value1),
				IndividualChange.createInsertChange(key2at3, value2)
		);
		Recoverable<TestValue> change = PendingBatchChange.createBatchChange(sortedChanges);
		getRecoveryManager().saveChange(change);
		List<TestValue> allValues = getTimeCollection().query().getList();
		assertEquals(0, allValues.size());

		getRecoveryManager().recover();
		allValues = getTimeCollection().query().getList();
		assertEquals(2, allValues.size());
		assertEquals(value1, allValues.get(0));
		assertEquals(value2, allValues.get(1));
	}

	@Test
	public void test_recover_pendingInsert_duplicate() throws Exception {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");

		getTimeCollection().insert(key,  value);
		PendingChange<TestValue> duplicateInsert = PendingChange.createInsert(key, value, serializer);
		getRecoveryManager().saveChange(duplicateInsert);
		List<TestValue> allValues = getTimeCollection().query().getList();
		assertEquals(1, allValues.size());

		getRecoveryManager().recover();
		allValues = getTimeCollection().query().getList();
		assertEquals(1, allValues.size());
		assertEquals(value, allValues.get(0));
	}

	@Test
	public void test_recover_pendingDelete() throws Exception {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");

		getTimeCollection().insert(key, value);
		List<TestValue> allValues = getTimeCollection().query().getList();
		assertEquals(1, allValues.size());

		PendingChange<TestValue> change = PendingChange.createDelete(key, value);
		getRecoveryManager().saveChange(change);
		getRecoveryManager().recover();
		allValues = getTimeCollection().query().getList();
		assertEquals(0, allValues.size());
	}

	@Test
	public void test_recover_pendingUpdate() throws Exception {
		BlueKey key = createKey(1, 2);
		TestValue originalValue = createValue("Joe", 0);
		Updater<TestValue> updater = ((v) -> v.addCupcake());
		TestValue newValue = serializer.clone(originalValue);
		updater.update(newValue);

		getTimeCollection().insert(key, originalValue);
		PendingChange<TestValue> change = PendingChange.createUpdate(key, originalValue, updater, serializer);
		getRecoveryManager().saveChange(change);

		List<TestValue> allValues = getTimeCollection().query().getList();
		assertEquals(1, allValues.size());
		assertEquals(0, allValues.get(0).getCupcakes());

		getRecoveryManager().recover();
		allValues = getTimeCollection().query().getList();
		assertEquals(1, allValues.size());
		assertEquals(1, allValues.get(0).getCupcakes());
	}

	@Test
	public void test_recover_invalidPendingChange() throws Exception {
		Path pathForGarbage = Paths.get(getTimeCollection().getPath().toString(), RecoveryManager.HISTORY_SUBFOLDER, "123" + RecoveryManager.SUFFIX);
		pathForGarbage.getParent().toFile().mkdirs();
		byte[] bytes = new byte[]{1, 2, 3};

		try (FileOutputStream fos = new FileOutputStream(pathForGarbage.toFile())) {
			fos.write(bytes);
			fos.close();
		}

		pathForGarbage.toFile().delete();
		getRecoveryManager().recover();
	}

	private Recoverable<TestValue> createRecoverable(long time){
		return new TestRecoverable(time);
	}
}
