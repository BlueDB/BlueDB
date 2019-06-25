package org.bluedb.disk.recovery;

import org.junit.Test;
import org.bluedb.api.Mapper;
import org.bluedb.api.Updater;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.segment.Segment;
import org.bluedb.disk.serialization.BlueEntity;

public class PendingChangeTest extends BlueDbDiskTestBase {

	@Test
	public void test_createDelete() {
		BlueKey key = createKey(1, 2);
		PendingChange<TestValue> change = PendingChange.createDelete(key, null);
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
		PendingChange<TestValue> change = PendingChange.createInsert(key, value, getSerializer());
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
		PendingChange<TestValue> change = PendingChange.createUpdate(key, initialValue, updater, getSerializer());
		TestValue newValue = getSerializer().clone(initialValue);
		updater.update(newValue);
		assertEquals(initialValue, change.getOldValue());
		assertEquals(newValue, change.getNewValue());
		assertFalse(change.isDelete());
		assertFalse(change.isInsert());
		assertTrue(change.isUpdate());
		assertEquals(key, change.getKey());
	}

	@Test
	public void test_applyChange_insert() throws Exception {
		BlueKey key = createKey(1, 2);
		removeKey(key);
		TestValue value = createValue("Joe");
		PendingChange<TestValue> change = PendingChange.createInsert(key, value, getSerializer());

		assertNull(getTimeCollection().get(key));
		Segment<TestValue> segment = getTimeCollection().getSegmentManager().getFirstSegment(key);
		change.applyChange(segment);
		assertEquals(value, getTimeCollection().get(key));


		removeKey(key);
	}

	@Test
	public void test_applyChange_delete() throws Exception {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		insertToTimeCollection(key, value);
		PendingChange<TestValue> change = PendingChange.createDelete(key, value);

		assertEquals(value, getTimeCollection().get(key));
		Segment<TestValue> segment = getTimeCollection().getSegmentManager().getFirstSegment(key);
		change.applyChange(segment);
		assertNull(getTimeCollection().get(key));


		removeKey(key);
	}

	@Test
	public void test_applyChange_update() throws Exception {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		insertToTimeCollection(key, value);
		Updater<TestValue> updater = ((v) -> v.addCupcake());
		TestValue newValue = getSerializer().clone(value);
		updater.update(newValue);
		PendingChange<TestValue> change = PendingChange.createUpdate(key, value, updater, getSerializer());

		assertEquals(value, getTimeCollection().get(key));
		Segment<TestValue> segment = getTimeCollection().getSegmentManager().getFirstSegment(key);
		change.applyChange(segment);
		assertEquals(newValue, getTimeCollection().get(key));


		removeKey(key);
	}

	@Test
	public void test_applyChange_replace() throws Exception {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		insertToTimeCollection(key, value);
		
		TestValue replacementValue = new TestValue(value.getName(), value.getCupcakes() + 1);
		Mapper<TestValue> updater = (v) -> {
			return replacementValue;
		};
		
		PendingChange<TestValue> change = PendingChange.createUpdate(new BlueEntity<TestValue>(key, value), updater, getSerializer());

		assertEquals(value, getTimeCollection().get(key));
		Segment<TestValue> segment = getTimeCollection().getSegmentManager().getFirstSegment(key);
		change.applyChange(segment);
		assertEquals(replacementValue, getTimeCollection().get(key));

		removeKey(key);
	}

	@Test
	public void test_applyChange_mutatingReplace() throws Exception {
		BlueKey key = createKey(1, 2);
		TestValue originalValue = createValue("Joe");
		TestValue originalCopy = createValue("Joe");
		insertToTimeCollection(key, originalValue);
		
		TestValue expectedAfterUpdate = new TestValue(originalValue.getName(), originalValue.getCupcakes() + 1);
		
		Mapper<TestValue> updater = (v) -> {
			v.addCupcake();
			return v;
		};
		
		PendingChange<TestValue> change = PendingChange.createUpdate(key, originalValue, updater, getSerializer());
		assertEquals(originalCopy, change.getOldValue());
		assertEquals(expectedAfterUpdate, change.getNewValue());

		assertEquals(originalValue, getTimeCollection().get(key));
		Segment<TestValue> segment = getTimeCollection().getSegmentManager().getFirstSegment(key);
		change.applyChange(segment);
		assertEquals(expectedAfterUpdate, getTimeCollection().get(key));

		removeKey(key);
	}

	@Test
	public void test_DeleteMultipleTask_toString() {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		Updater<TestValue> updater = ((v) -> v.addCupcake());
		TestValue newValue = getSerializer().clone(value);
		PendingChange<TestValue> change = PendingChange.createUpdate(key, value, updater, getSerializer());
		String changeString = change.toString();
		assertTrue(changeString.contains(change.getClass().getSimpleName()));
		assertTrue(changeString.contains(key.toString()));
		assertTrue(changeString.contains(value.toString()));
		assertTrue(changeString.contains(newValue.toString()));
	}
}
