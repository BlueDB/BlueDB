package io.bluedb.disk.recovery;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import org.junit.Test;
import io.bluedb.api.Updater;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.BlueDbOnDisk;
import io.bluedb.disk.BlueDbOnDiskBuilder;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.collection.BlueCollectionImpl;
import io.bluedb.disk.file.FileManager;
import io.bluedb.disk.serialization.BlueSerializer;
import io.bluedb.disk.serialization.ThreadLocalFstSerializer;
import junit.framework.TestCase;

public class RecoveryManagerTest extends TestCase {

	BlueDbOnDisk DB;
	BlueCollectionImpl<TestValue> COLLECTION;
	RecoveryManager<TestValue> recoveryManager;
	BlueSerializer serializer;
	
	
	@Override
	public void setUp() throws Exception {
		DB = new BlueDbOnDiskBuilder().build();
		COLLECTION = (BlueCollectionImpl<TestValue>) DB.getCollection(TestValue.class, "testing");
		recoveryManager = COLLECTION.getRecoveryManager();
		serializer = new ThreadLocalFstSerializer(new Class[] {});

		List<PendingChange<TestValue>> changes = recoveryManager.getPendingChanges();
		for (PendingChange<TestValue> change: changes) {
			recoveryManager.removeChange(change);
		}
		
		COLLECTION.query().delete();
	}

	@Override
	public void tearDown() throws Exception {
		List<PendingChange<TestValue>> changes = recoveryManager.getPendingChanges();
		for (PendingChange<TestValue> change: changes) {
			recoveryManager.removeChange(change);
		}

		COLLECTION.query().delete();
	}

	@Test
	public void test_getFileName() {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
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
			List<PendingChange<TestValue>> changes = recoveryManager.getPendingChanges();
			assertEquals(0, changes.size());
			recoveryManager.saveChange(change);
			changes = recoveryManager.getPendingChanges();
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
			recoveryManager.saveChange(change);
			List<PendingChange<TestValue>> changes = recoveryManager.getPendingChanges();
			assertEquals(1, changes.size());
			recoveryManager.removeChange(change);
			changes = recoveryManager.getPendingChanges();
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
			List<PendingChange<TestValue>> changes = recoveryManager.getPendingChanges();
			assertEquals(0, changes.size());
			recoveryManager.saveChange(change);
			changes = recoveryManager.getPendingChanges();
			assertEquals(1, changes.size());
			recoveryManager.removeChange(change);
			changes = recoveryManager.getPendingChanges();
			assertEquals(0, changes.size());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}	}

//	@Test
//	public void test_saveInsert() {
//		BlueKey key = createKey(1, 2);
//		TestValue value = createValue("Joe");
//		try {
//			List<PendingChange<TestValue>> changes = recoveryManager.getPendingChanges();
//			assertEquals(0, changes.size());
//
//			PendingChange<TestValue> change = recoveryManager.saveInsert(key, value);
//			assertTrue(change.isInsert());
//			assertEquals(key, change.getKey());
//			assertEquals(value, change.getNewValue());
//			assertTrue(value != change.getOldValue());  // clone, not original object
//
//			changes = recoveryManager.getPendingChanges();
//			assertEquals(1, changes.size());
//			PendingChange<TestValue> savedChange = changes.get(0);
//			assertTrue(savedChange.isInsert());
//			assertEquals(key, savedChange.getKey());
//			assertEquals(value, savedChange.getNewValue());
//		} catch (BlueDbException e) {
//			e.printStackTrace();
//			fail();
//		}
//	}
//
//	@Test
//	public void test_saveDelete() {
//		BlueKey key = createKey(1, 2);
//		try {
//			List<PendingChange<TestValue>> changes = recoveryManager.getPendingChanges();
//			assertEquals(0, changes.size());
//
//			PendingChange<TestValue> change = recoveryManager.saveDelete(key);
//			assertTrue(change.isDelete());
//			assertEquals(key, change.getKey());
//
//			changes = recoveryManager.getPendingChanges();
//			assertEquals(1, changes.size());
//			PendingChange<TestValue> savedChange = changes.get(0);
//			assertTrue(savedChange.isDelete());
//			assertEquals(key, savedChange.getKey());
//		} catch (BlueDbException e) {
//			e.printStackTrace();
//			fail();
//		}
//	}
//
//	@Test
//	public void test_saveUpdate() {
//		BlueKey key = createKey(1, 2);
//		TestValue originalValue = createValue("Joe");
//		Updater<TestValue> updater = ((v) -> v.addCupcake());
//		TestValue newValue = serializer.clone(originalValue);
//		updater.update(newValue);
//		try {
//			List<PendingChange<TestValue>> changes = recoveryManager.getPendingChanges();
//			assertEquals(0, changes.size());
//
//			BlueEntity<TestValue> entity = new BlueEntity<>(key, originalValue);
//			PendingChange<TestValue> change = PendingChange.createUpdate(entity, updater, serializer);
//			recoveryManager.saveChange(change);
////			PendingChange<TestValue> change = recoveryManager.saveUpdate(key, originalValue, updater);
//			assertTrue(change.isUpdate());
//			assertEquals(key, change.getKey());
//			assertEquals(originalValue, change.getOldValue());
//			assertTrue(originalValue != change.getOldValue());  // clone, not original object
//			assertEquals(newValue, change.getNewValue());
//
//			changes = recoveryManager.getPendingChanges();
//			assertEquals(1, changes.size());
//			PendingChange<TestValue> savedChange = changes.get(0);
//			assertTrue(savedChange.isUpdate());
//			assertEquals(key, savedChange.getKey());
//			assertEquals(originalValue, savedChange.getOldValue());
//			assertEquals(newValue, savedChange.getNewValue());
//		} catch (BlueDbException e) {
//			e.printStackTrace();
//			fail();
//		}
//	}

	@Test
	public void test_recover_pendingInsert() {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		try {
			PendingChange<TestValue> change = PendingChange.createInsert(key, value, serializer);
			recoveryManager.saveChange(change);
			List<TestValue> allValues = COLLECTION.query().getList();
			assertEquals(0, allValues.size());
			
			recoveryManager.recover();
			allValues = COLLECTION.query().getList();
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
			COLLECTION.insert(key, value);
			List<TestValue> allValues = COLLECTION.query().getList();
			assertEquals(1, allValues.size());
			
			PendingChange<TestValue> change = PendingChange.createDelete(key);
			recoveryManager.saveChange(change);
			recoveryManager.recover();
			allValues = COLLECTION.query().getList();
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
			COLLECTION.insert(key, originalValue);
			PendingChange<TestValue> change = PendingChange.createUpdate(key, originalValue, updater, serializer);
			recoveryManager.saveChange(change);

			List<TestValue> allValues = COLLECTION.query().getList();
			assertEquals(1, allValues.size());
			assertEquals(0, allValues.get(0).getCupcakes());

			recoveryManager.recover();
			allValues = COLLECTION.query().getList();
			assertEquals(1, allValues.size());
			assertEquals(1, allValues.get(0).getCupcakes());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_recover_invalidPendingChange() {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");

		Path pathForGarbage = Paths.get(COLLECTION.getPath().toString(), RecoveryManager.SUBFOLDER, "123" + RecoveryManager.SUFFIX);
		byte[] bytes = new byte[]{1, 2, 3};
		try (FileOutputStream fos = new FileOutputStream(pathForGarbage.toFile())) {
			fos.write(bytes);
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}

		pathForGarbage.toFile().delete();
		recoveryManager.recover();
	}
	private TestValue createValue(String name, int cupcakes){
		return new TestValue(name, cupcakes);
	}

	private TestValue createValue(String name){
		return new TestValue(name);
	}

	private BlueKey createKey(long keyId, long time){
		return new TimeKey(keyId, time);
	}
}
