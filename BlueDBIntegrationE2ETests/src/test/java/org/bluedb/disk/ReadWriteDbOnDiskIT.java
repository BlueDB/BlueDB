package org.bluedb.disk;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.collection.ReadWriteTimeCollectionOnDisk;
import org.bluedb.disk.helpers.BlueDbOnDiskWrapper;
import org.bluedb.zip.ZipUtils;
import org.junit.Test;
import static org.junit.Assert.*;

public class ReadWriteDbOnDiskIT {

	@Test
	public void encryptedDb_writeAndQueryValues_successfullyEncryptsAndDecrypts() throws IOException, BlueDbException {
		try (BlueDbOnDiskWrapper dbWrapper = new BlueDbOnDiskWrapper(BlueDbOnDiskWrapper.StartupOption.EncryptionEnabled, null)) {
			BlueKey key1At1 = new TimeKey(1, 1);
			TestValue value1Anna = new TestValue("Anna");
			dbWrapper.getTimeCollection().insert(key1At1, value1Anna);

			BlueKey key2At2 = new TimeKey(2, 2);
			TestValue value2Bob = new TestValue("Bob");
			dbWrapper.getTimeCollection().insert(key2At2, value2Bob);

			List<TestValue> expected = Collections.singletonList(value2Bob);
			List<TestValue> actual = dbWrapper.getTimeCollection().query().where((v) -> v.getName().equals("Bob")).getList();

			assertEquals(expected, actual);
		}
	}

	@Test
	public void encryptedDb_writeThenDisableEncryptionThenQuery_successfullyDecryptsAsNeeded() throws IOException, BlueDbException {
		try (BlueDbOnDiskWrapper dbWrapper = new BlueDbOnDiskWrapper(BlueDbOnDiskWrapper.StartupOption.EncryptionEnabled, null)) {
			BlueKey key1At1 = new TimeKey(1, 1);
			TestValue value1Anna = new TestValue("Anna");
			dbWrapper.getTimeCollection().insert(key1At1, value1Anna);

			BlueKey key2At2 = new TimeKey(2, 2);
			TestValue value2Bob = new TestValue("Bob");
			dbWrapper.getTimeCollection().insert(key2At2, value2Bob);

			dbWrapper.getEncryptionService().setEncryptionEnabled(false);

			List<TestValue> expected = Collections.singletonList(value2Bob);
			List<TestValue> actual = dbWrapper.getTimeCollection().query().where((v) -> v.getName().equals("Bob")).getList();

			assertEquals(expected, actual);
		}
	}

	@Test
	public void encryptedDb_backupThenRestoreThenQuery_success() throws BlueDbException, IOException {
		try (BlueDbOnDiskWrapper dbWrapper = new BlueDbOnDiskWrapper(BlueDbOnDiskWrapper.StartupOption.EncryptionEnabled, null)) {
			BlueKey key1At1 = new TimeKey(1, 1);
			TestValue value1 = new TestValue("Anna");
			dbWrapper.getTimeCollection().insert(key1At1, value1);

			ReadWriteTimeCollectionOnDisk<TestValue2> secondCollection = (ReadWriteTimeCollectionOnDisk<TestValue2>) dbWrapper.getDb().getTimeCollectionBuilder("testing_2", TimeKey.class, TestValue2.class).build();
			TestValue2 valueInSecondCollection = new TestValue2("Joe", 3);
			secondCollection.insert(key1At1, valueInSecondCollection);

			Path tempFolder = dbWrapper.createTempFolder().toPath();
			Path backupPath = Paths.get(tempFolder.toString(), "backup_test.zip");

			dbWrapper.getDb().backup(backupPath);

			Path restoredPath = Paths.get(tempFolder.toString(), "restore_test");
			ZipUtils.extractFiles(backupPath, restoredPath);
			Path restoredBlueDbPath = Paths.get(restoredPath.toString(), "bluedb");

			try (BlueDbOnDiskWrapper restoredDbWrapper = new BlueDbOnDiskWrapper(BlueDbOnDiskWrapper.StartupOption.EncryptionEnabled, restoredBlueDbPath, null)) {

				ReadWriteTimeCollectionOnDisk<TestValue> restoredCollection = restoredDbWrapper.getTimeCollection();
				assertTrue(restoredCollection.contains(key1At1));
				assertEquals(value1, restoredCollection.get(key1At1));

				ReadWriteTimeCollectionOnDisk<TestValue2> secondCollectionRestored = (ReadWriteTimeCollectionOnDisk<TestValue2>) restoredDbWrapper.getDb().getTimeCollectionBuilder("testing_2", TimeKey.class, TestValue2.class).build();
				assertTrue(secondCollectionRestored.contains(key1At1));
				assertEquals(valueInSecondCollection, secondCollectionRestored.get(key1At1));
			}
		}
	}

}
