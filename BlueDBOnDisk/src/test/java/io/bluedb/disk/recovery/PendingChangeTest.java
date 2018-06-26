package io.bluedb.disk.recovery;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.bluedb.api.Updater;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.serialization.BlueSerializer;
import io.bluedb.disk.serialization.ThreadLocalFstSerializer;

public class PendingChangeTest {

	BlueSerializer serializer;
	
	@Before
	public void setUp() throws Exception {
		serializer = new ThreadLocalFstSerializer(new Class[] {});
	}

//	@After
//	public void tearDown() throws Exception {}
//
	@Test
	public void test_createDelete() {
		BlueKey key = createKey(1, 2);
		PendingChange<TestValue> change = PendingChange.createDelete(key);
		assertNull(change.getOldValue());
		assertNull(change.getNewValue());
		assertTrue(change.isDelete());
		assertFalse(change.isInsert());
		assertFalse(change.isUpdate());
		assertEquals(key, change.getKey());
	}

	@Test
	public void test_createInsert() {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		PendingChange<TestValue> change = PendingChange.createInsert(key, value);
		assertNull(change.getOldValue());
		assertEquals(value, change.getNewValue());
		assertFalse(change.isDelete());
		assertTrue(change.isInsert());
		assertFalse(change.isUpdate());
		assertEquals(key, change.getKey());
	}
	
	@Test
	public void test_createUpdate() {
		BlueKey key = createKey(1, 2);
		TestValue initialValue = createValue("Joe");
		Updater<TestValue> updater = ((v) -> v.addCupcake());
		TestValue newValue = serializer.clone(initialValue);
		updater.update(newValue);
		PendingChange<TestValue> change = PendingChange.createUpdate(key, initialValue, newValue);
		assertEquals(initialValue, change.getOldValue());
		assertEquals(newValue, change.getNewValue());
		assertFalse(change.isDelete());
		assertFalse(change.isInsert());
		assertTrue(change.isUpdate());
		assertEquals(key, change.getKey());
	}

	@Test
	public void test_applyChange_insert() {
		// TODO
	}

	@Test
	public void test_applyChange_delete() {
		// TODO
	}

	@Test
	public void test_applyChange_update() {
		// TODO
	}

	private TestValue createValue(String name){
		return new TestValue(name);
	}

	private BlueKey createKey(long keyId, long time){
		return new TimeKey(keyId, time);
	}
}
