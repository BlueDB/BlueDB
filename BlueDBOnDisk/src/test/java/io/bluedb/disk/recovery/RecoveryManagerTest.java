package io.bluedb.disk.recovery;

import static org.junit.Assert.*;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.bluedb.api.Updater;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.BlueDbOnDisk;
import io.bluedb.disk.BlueDbOnDiskBuilder;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.collection.BlueCollectionImpl;
import io.bluedb.disk.serialization.BlueSerializer;
import io.bluedb.disk.serialization.ThreadLocalFstSerializer;
import junit.framework.TestCase;

public class RecoveryManagerTest extends TestCase {

	BlueDbOnDisk DB = new BlueDbOnDiskBuilder().build();
	BlueCollectionImpl<TestValue> COLLECTION = (BlueCollectionImpl<TestValue>) DB.getCollection(TestValue.class, "testing");
	RecoveryManager<TestValue> recoveryManager;
	BlueSerializer serializer;
	
	
	@Override
	public void setUp() throws Exception {
		recoveryManager = COLLECTION.getRecoveryManager();
		serializer = new ThreadLocalFstSerializer(new Class[] {});

		List<PendingChange<TestValue>> changes = recoveryManager.getPendingChanges();
		for (PendingChange<TestValue> change: changes) {
			recoveryManager.removeChange(change);
		}
	}

	@After
	public void tearDown() throws Exception {
		List<PendingChange<TestValue>> changes = recoveryManager.getPendingChanges();
		for (PendingChange<TestValue> change: changes) {
			recoveryManager.removeChange(change);
		}
	}

	@Test
	public void test_getFileName() {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		PendingChange<TestValue> change = PendingChange.createInsert(key, value);
		String fileName1 = RecoveryManager.getFileName(change);
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail();
		}
		PendingChange<TestValue> change2 = PendingChange.createInsert(key, value);
		String fileName2 = RecoveryManager.getFileName(change2);
		assertTrue(fileName1.compareTo(fileName2) < 0);
	}

	@Test
	public void test_saveChange() {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		PendingChange<TestValue> change = PendingChange.createInsert(key, value);
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
		PendingChange<TestValue> change = PendingChange.createInsert(key, value);
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
		PendingChange<TestValue> change = PendingChange.createInsert(key, value);
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

	@Test
	public void test_saveInsert() {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		try {
			List<PendingChange<TestValue>> changes = recoveryManager.getPendingChanges();
			assertEquals(0, changes.size());

			PendingChange<TestValue> change = recoveryManager.saveInsert(key, value);
			assertTrue(change.isInsert());
			assertEquals(key, change.getKey());
			assertEquals(value, change.getNewValue());
			assertTrue(value != change.getOldValue());  // clone, not original object

			changes = recoveryManager.getPendingChanges();
			assertEquals(1, changes.size());
			PendingChange<TestValue> savedChange = changes.get(0);
			assertTrue(savedChange.isInsert());
			assertEquals(key, savedChange.getKey());
			assertEquals(value, savedChange.getNewValue());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_saveDelete() {
		BlueKey key = createKey(1, 2);
		try {
			List<PendingChange<TestValue>> changes = recoveryManager.getPendingChanges();
			assertEquals(0, changes.size());

			PendingChange<TestValue> change = recoveryManager.saveDelete(key);
			assertTrue(change.isDelete());
			assertEquals(key, change.getKey());

			changes = recoveryManager.getPendingChanges();
			assertEquals(1, changes.size());
			PendingChange<TestValue> savedChange = changes.get(0);
			assertTrue(savedChange.isDelete());
			assertEquals(key, savedChange.getKey());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_saveUpdate() {
		BlueKey key = createKey(1, 2);
		TestValue originalValue = createValue("Joe");
		Updater<TestValue> updater = ((v) -> v.addCupcake());
		TestValue newValue = serializer.clone(originalValue);
		updater.update(newValue);
		try {
			List<PendingChange<TestValue>> changes = recoveryManager.getPendingChanges();
			assertEquals(0, changes.size());

			PendingChange<TestValue> change = recoveryManager.saveUpdate(key, originalValue, updater);
			assertTrue(change.isUpdate());
			assertEquals(key, change.getKey());
			assertEquals(originalValue, change.getOldValue());
			assertTrue(originalValue != change.getOldValue());  // clone, not original object
			assertEquals(newValue, change.getNewValue());

			changes = recoveryManager.getPendingChanges();
			assertEquals(1, changes.size());
			PendingChange<TestValue> savedChange = changes.get(0);
			assertTrue(savedChange.isUpdate());
			assertEquals(key, savedChange.getKey());
			assertEquals(originalValue, savedChange.getOldValue());
			assertEquals(newValue, savedChange.getNewValue());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_recover_noChanges() {
		// TODO
	}

	@Test
	public void test_recover_pendingChanges() {
		// TODO
	}

	private TestValue createValue(String name){
		return new TestValue(name);
	}

	private BlueKey createKey(long keyId, long time){
		return new TimeKey(keyId, time);
	}
}