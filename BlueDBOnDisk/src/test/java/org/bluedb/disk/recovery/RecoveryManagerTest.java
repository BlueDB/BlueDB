package org.bluedb.disk.recovery;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.bluedb.TestCloseableIterator;
import org.bluedb.api.CloseableIterator;
import org.bluedb.api.Updater;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.StreamUtils;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.config.TestDefaultConfigurationService;
import org.bluedb.disk.file.FileUtils;
import org.bluedb.disk.query.QueryOnDisk;
import org.bluedb.disk.serialization.BlueEntity;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;
import org.junit.Test;
import org.mockito.Mockito;

public class RecoveryManagerTest extends BlueDbDiskTestBase {

	BlueSerializer serializer;

	@Override
	public void setUp() throws Exception {
		super.setUp();
		serializer = new ThreadLocalFstSerializer(new TestDefaultConfigurationService(), new Class[] {});
	}

	@Test
	public void test_getPendingFileName() throws Exception {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		BlueSerializer serializer = new ThreadLocalFstSerializer(new TestDefaultConfigurationService(), new Class[] {});
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
		BlueSerializer serializer = new ThreadLocalFstSerializer(new TestDefaultConfigurationService(), new Class[] {});
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
		getRecoveryManager().saveNewChange(change);
		changes = getRecoveryManager().getPendingChanges();
		PendingChange<TestValue> savedChange = (PendingChange<TestValue>) changes.get(0);
		assertEquals(1, changes.size());
		assertEquals(value, savedChange.getNewValue());
	}

	@Test
	public void test_saveMassChangeForQueryChange() throws Exception {
		List<BlueEntity<TestValue>> sortedEntitiesToUpdate = new LinkedList<>();
		for(int i = 0; i < 10; i++) {
			sortedEntitiesToUpdate.add(new BlueEntity<TestValue>(new TimeKey(i, i), new TestValue(String.valueOf(i), i)));
		}
		CloseableIterator<BlueEntity<TestValue>> sortedEntitiesToUpdateIterator = new TestCloseableIterator<>(sortedEntitiesToUpdate.iterator());
		
		@SuppressWarnings("unchecked")
		QueryOnDisk<TestValue> mockedQuery = (QueryOnDisk<TestValue>) Mockito.mock(QueryOnDisk.class);
		Mockito.doReturn(sortedEntitiesToUpdateIterator).when(mockedQuery).getEntityIterator();
		
		EntityToChangeMapper<TestValue> entityToChangeMapper = entity -> {
			return IndividualChange.createUpdateChange(entity.getKey(), entity.getValue(), value -> value.addCupcake(), serializer);
		};

		List<Recoverable<TestValue>> changes = getRecoveryManager().getPendingChanges();
		assertEquals(0, changes.size());
		getRecoveryManager().saveMassChangeForQueryChange(mockedQuery, entityToChangeMapper);
		changes = getRecoveryManager().getPendingChanges();
		PendingMassChange<TestValue> savedChange = (PendingMassChange<TestValue>) changes.get(0);
		assertEquals(1, changes.size());
		
		List<IndividualChange<TestValue>> expectedChanges = StreamUtils.stream(sortedEntitiesToUpdate)
			.map(entity -> {
				try {
					return entityToChangeMapper.map(entity);
				} catch (BlueDbException e) {
					e.printStackTrace();
					return null;
				}
			})
			.filter(Objects::nonNull)
			.collect(Collectors.toList());
		
		OnDiskSortedChangeSupplier<TestValue> sortedChangeSupplier = new OnDiskSortedChangeSupplier<TestValue>(savedChange.getChangesFilePath(), getFileManager());
		SortedChangeIterator<TestValue> sortedChangeIterator = new SortedChangeIterator<TestValue>(sortedChangeSupplier);
		List<IndividualChange<TestValue>> actualChanges = new LinkedList<>();
		while(sortedChangeIterator.hasNext()) {
			actualChanges.add(sortedChangeIterator.next());
		}
		
		assertEquals(expectedChanges, actualChanges);
	}
	
	@Test
	public void test_saveMassChangeForBatchUpsert() throws BlueDbException {
		List<IndividualChange<TestValue>> originalSortedChangeList = new LinkedList<>();
		originalSortedChangeList.add(null); //Null should be skipped but the rest should go through
		for(int i = 0; i < 10; i++) {
			TimeKey key = new TimeKey(i, i);
			TestValue oldValue = new TestValue(String.valueOf(i), i-1);
			TestValue newValue = new TestValue(String.valueOf(i), i);
			
			if(i % 2 == 0) {
				originalSortedChangeList.add(IndividualChange.createInsertChange(key, newValue));
			} else {
				originalSortedChangeList.add(IndividualChange.manuallyCreateTestChange(key, oldValue, newValue, Optional.empty()));
			}
		}
		
		List<Recoverable<TestValue>> pendingChanges = getRecoveryManager().getPendingChanges();
		assertEquals(0, pendingChanges.size());
		
		getRecoveryManager().saveMassChangeForBatchUpsert(originalSortedChangeList.iterator());
		pendingChanges = getRecoveryManager().getPendingChanges();
		PendingMassChange<TestValue> savedChange = (PendingMassChange<TestValue>) pendingChanges.get(0);
		assertEquals(1, pendingChanges.size());
		
		OnDiskSortedChangeSupplier<TestValue> sortedChangeSupplier = new OnDiskSortedChangeSupplier<TestValue>(savedChange.getChangesFilePath(), getFileManager());
		SortedChangeIterator<TestValue> sortedChangeIterator = new SortedChangeIterator<TestValue>(sortedChangeSupplier);
		List<IndividualChange<TestValue>> actualChanges = new LinkedList<>();
		while(sortedChangeIterator.hasNext()) {
			actualChanges.add(sortedChangeIterator.next());
		}
		
		List<IndividualChange<TestValue>> expectedChanges = originalSortedChangeList.subList(1, originalSortedChangeList.size()); //Remove the null since it shouldn't have gone through
		assertEquals(expectedChanges, actualChanges);
	}
	
	@Test
	public void test_saveMassChangeForBatchUpsert_invalidIterator() throws BlueDbException {
		@SuppressWarnings("unchecked")
		Iterator<IndividualChange<TestValue>> mockedIteratorThatWillThrowException = (Iterator<IndividualChange<TestValue>>) Mockito.mock(Iterator.class);
		Mockito.doReturn(true).when(mockedIteratorThatWillThrowException).hasNext();
		Mockito.doThrow(new RuntimeException()).when(mockedIteratorThatWillThrowException).next();
		
		List<Recoverable<TestValue>> pendingChanges = getRecoveryManager().getPendingChanges();
		assertEquals(0, pendingChanges.size());
		
		try {
			getRecoveryManager().saveMassChangeForBatchUpsert(mockedIteratorThatWillThrowException);
			fail();
		} catch(RuntimeException e) {
			//expected
		}
		
		assertTrue(getRecoveryManager().getPendingChangeFiles().isEmpty());
		
		pendingChanges = getRecoveryManager().getPendingChanges();
		assertEquals(0, pendingChanges.size());
	}

	@Test
	public void test_markComplete() throws Exception {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		Recoverable<TestValue> change = PendingChange.createInsert(key, value, serializer);

		getRecoveryManager().saveNewChange(change);
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
		getRecoveryManager().saveNewChange(change);
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

		getRecoveryManager().saveNewChange(change);
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
		
		List<File> completedChanges = new LinkedList<File>();
		getRecoveryManager().getCompletedChangeFilesAsStream().iterator().forEachRemaining((p) -> completedChanges.add(p.toFile()));
		assertEquals(1, completedChanges.size());

		File completedChangeFile = completedChanges.get(0);
		File tempCompletedFile = FileUtils.createTempFilePath(completedChangeFile.toPath()).toFile();
		tempCompletedFile.createNewFile();
		allFilesInChangeFolder = Arrays.asList(historyFolderPath.toFile().listFiles());
		assertEquals(3, allFilesInChangeFolder.size());  // [pending change temp file, complete change temp file, complete change file]
		completedChanges.clear();
		getRecoveryManager().getCompletedChangeFilesAsStream().iterator().forEachRemaining((p) -> completedChanges.add(p.toFile()));
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
		getRecoveryManager().saveNewChange(change30);
		getRecoveryManager().saveNewChange(change60);
		getRecoveryManager().saveNewChange(change90);
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
		getRecoveryManager().saveNewChange(change);
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
		getRecoveryManager().saveNewChange(change);
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
		getRecoveryManager().saveNewChange(duplicateInsert);
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
		getRecoveryManager().saveNewChange(change);
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
		getRecoveryManager().saveNewChange(change);

		List<TestValue> allValues = getTimeCollection().query().getList();
		assertEquals(1, allValues.size());
		assertEquals(0, allValues.get(0).getCupcakes());

		getRecoveryManager().recover();
		allValues = getTimeCollection().query().getList();
		assertEquals(1, allValues.size());
		assertEquals(1, allValues.get(0).getCupcakes());
	}

	@Test
	public void test_recover_invalidChange() throws Exception {
		Path pathForGarbage = Paths.get(getTimeCollection().getPath().toString(), RecoveryManager.RECOVERY_FOLDER, RecoveryManager.HISTORY_SUBFOLDER, "123" + RecoveryManager.SUFFIX);
		pathForGarbage.getParent().toFile().mkdirs();
		byte[] bytes = new byte[]{1, 2, 3};

		try (FileOutputStream fos = new FileOutputStream(pathForGarbage.toFile())) {
			fos.write(bytes);
			fos.close();
		}

		getRecoveryManager().recover();
	}

	@Test
	public void test_recover_invalidPendingChange() throws Exception {
		Path pathForGarbage = Paths.get(getTimeCollection().getPath().toString(), RecoveryManager.RECOVERY_FOLDER, RecoveryManager.HISTORY_SUBFOLDER, "123" + RecoveryManager.SUFFIX_PENDING);
		pathForGarbage.getParent().toFile().mkdirs();
		byte[] bytes = new byte[]{1, 2, 3};

		try (FileOutputStream fos = new FileOutputStream(pathForGarbage.toFile())) {
			fos.write(bytes);
			fos.close();
		}

		getRecoveryManager().recover();
	}

	@Test
	public void test_recover_emptyChange() throws Exception {
		Path pathForGarbage = Paths.get(getTimeCollection().getPath().toString(), RecoveryManager.RECOVERY_FOLDER, RecoveryManager.HISTORY_SUBFOLDER, "empty" + RecoveryManager.SUFFIX);
		pathForGarbage.getParent().toFile().mkdirs();
		byte[] bytes = new byte[]{};

		try (FileOutputStream fos = new FileOutputStream(pathForGarbage.toFile())) {
			fos.write(bytes);
			fos.close();
		}

		getRecoveryManager().recover();
	}

	@Test
	public void test_recover_emptyPendingChange() throws Exception {
		Path pathForGarbage = Paths.get(getTimeCollection().getPath().toString(), RecoveryManager.RECOVERY_FOLDER, RecoveryManager.HISTORY_SUBFOLDER, "empty" + RecoveryManager.SUFFIX_PENDING);
		pathForGarbage.getParent().toFile().mkdirs();
		byte[] bytes = new byte[]{};

		try (FileOutputStream fos = new FileOutputStream(pathForGarbage.toFile())) {
			fos.write(bytes);
			fos.close();
		}

		getRecoveryManager().recover();
	}

	private Recoverable<TestValue> createRecoverable(long time){
		return new TestRecoverable(time);
	}
}
