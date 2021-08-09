package org.bluedb.disk;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.helpers.BlueDbOnDiskWrapper;
import org.junit.Test;
import static org.junit.Assert.*;

public class ReadWriteDbOnDiskIT {
	
	@Test
	public void encryptedDb_writeAndQueryValues_successfullyEncryptsAndDecrypts() throws IOException, BlueDbException {
		try (BlueDbOnDiskWrapper dbWrapper = new BlueDbOnDiskWrapper(BlueDbOnDiskWrapper.StartupOption.EncryptionEnabled)) {
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
	public void encryptedDb_writeThenDisableEncryptionEncryption_successfullyDecryptsAsNeeded() throws IOException, BlueDbException {
		try (BlueDbOnDiskWrapper dbWrapper = new BlueDbOnDiskWrapper(BlueDbOnDiskWrapper.StartupOption.EncryptionEnabled)) {
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
}
