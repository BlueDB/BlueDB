package io.bluedb.disk.recovery;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Test;
import io.bluedb.api.Updater;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.serialization.BlueSerializer;
import io.bluedb.disk.serialization.ThreadLocalFstSerializer;

public class RecoveryManagerTest extends BlueDbDiskTestBase {

	BlueSerializer serializer;

	@Override
	public void setUp() throws Exception {
		super.setUp();
		serializer = new ThreadLocalFstSerializer(new Class[] {});
	}

	@Test
	public void test_getFileName() {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		BlueSerializer serializer = new ThreadLocalFstSerializer(new Class[] {});
		PendingChange<TestValue> change = PendingChange.createInsert(key, value, serializer);
		String fileName1 = RecoveryManager.getFileName(change);
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail();
		}
		PendingChange<TestValue> change2 = PendingChange.createInsert(key, value, serializer);
		String fileName2 = RecoveryManager.getFileName(change2);
		assertTrue(fileName1.compareTo(fileName2) < 0);
	}

	@Test
	public void test_saveChange() {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		PendingChange<TestValue> change = PendingChange.createInsert(key, value, serializer);
		try {
			List<PendingChange<TestValue>> changes = getRecoveryManager().getPendingChanges();
			assertEquals(0, changes.size());
			getRecoveryManager().saveChange(change);
			changes = getRecoveryManager().getPendingChanges();
			assertEquals(1, changes.size());
			assertEquals(value, changes.get(0).getNewValue());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_removeChange() {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		PendingChange<TestValue> change = PendingChange.createInsert(key, value, serializer);
		try {
			getRecoveryManager().saveChange(change);
			List<PendingChange<TestValue>> changes = getRecoveryManager().getPendingChanges();
			assertEquals(1, changes.size());
			getRecoveryManager().removeChange(change);
			changes = getRecoveryManager().getPendingChanges();
			assertEquals(0, changes.size());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_getPendingChanges() {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		PendingChange<TestValue> change = PendingChange.createInsert(key, value, serializer);
		try {
			List<PendingChange<TestValue>> changes = getRecoveryManager().getPendingChanges();
			assertEquals(0, changes.size());
			getRecoveryManager().saveChange(change);
			changes = getRecoveryManager().getPendingChanges();
			assertEquals(1, changes.size());
			getRecoveryManager().removeChange(change);
			changes = getRecoveryManager().getPendingChanges();
			assertEquals(0, changes.size());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}	}

	@Test
	public void test_recover_pendingInsert() {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		try {
			PendingChange<TestValue> change = PendingChange.createInsert(key, value, serializer);
			getRecoveryManager().saveChange(change);
			List<TestValue> allValues = getCollection().query().getList();
			assertEquals(0, allValues.size());

			getRecoveryManager().recover();
			allValues = getCollection().query().getList();
			assertEquals(1, allValues.size());
			assertEquals(value, allValues.get(0));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_recover_pendingDelete() {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		try {
			getCollection().insert(key, value);
			List<TestValue> allValues = getCollection().query().getList();
			assertEquals(1, allValues.size());

			PendingChange<TestValue> change = PendingChange.createDelete(key);
			getRecoveryManager().saveChange(change);
			getRecoveryManager().recover();
			allValues = getCollection().query().getList();
			assertEquals(0, allValues.size());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_recover_pendingUpdate() {
		BlueKey key = createKey(1, 2);
		TestValue originalValue = createValue("Joe", 0);
		Updater<TestValue> updater = ((v) -> v.addCupcake());
		TestValue newValue = serializer.clone(originalValue);
		updater.update(newValue);
		try {
			getCollection().insert(key, originalValue);
			PendingChange<TestValue> change = PendingChange.createUpdate(key, originalValue, updater, serializer);
			getRecoveryManager().saveChange(change);

			List<TestValue> allValues = getCollection().query().getList();
			assertEquals(1, allValues.size());
			assertEquals(0, allValues.get(0).getCupcakes());

			getRecoveryManager().recover();
			allValues = getCollection().query().getList();
			assertEquals(1, allValues.size());
			assertEquals(1, allValues.get(0).getCupcakes());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_recover_invalidPendingChange() {
		Path pathForGarbage = Paths.get(getCollection().getPath().toString(), RecoveryManager.SUBFOLDER, "123" + RecoveryManager.SUFFIX);
		pathForGarbage.getParent().toFile().mkdirs();
		byte[] bytes = new byte[]{1, 2, 3};
		try (FileOutputStream fos = new FileOutputStream(pathForGarbage.toFile())) {
			fos.write(bytes);
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}

		pathForGarbage.toFile().delete();
		getRecoveryManager().recover();
	}
}
