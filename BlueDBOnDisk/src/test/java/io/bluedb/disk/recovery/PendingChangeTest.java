package io.bluedb.disk.recovery;

import static org.junit.Assert.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

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
import io.bluedb.disk.segment.Segment;
import io.bluedb.disk.serialization.BlueSerializer;
import io.bluedb.disk.serialization.ThreadLocalFstSerializer;

public class PendingChangeTest {

	BlueDbOnDisk DB;
	BlueCollectionImpl<TestValue> COLLECTION;
	BlueSerializer serializer;
	Path dbPath;
	
	@Before
	public void setUp() throws Exception {
		dbPath = Paths.get("testing_PendingChangeTest");
		DB = new BlueDbOnDiskBuilder().setPath(dbPath).build();
		COLLECTION = (BlueCollectionImpl<TestValue>) DB.getCollection(TestValue.class, "testing");
		serializer = new ThreadLocalFstSerializer(new Class[] {});
	}

	@After
	public void tearDown() throws Exception {
		Files.walk(dbPath)
		.sorted(Comparator.reverseOrder())
		.map(Path::toFile)
		.forEach(File::delete);
	}

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
		PendingChange<TestValue> change = PendingChange.createInsert(key, value, serializer);
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
		PendingChange<TestValue> change = PendingChange.createUpdate(key, initialValue, updater, serializer);
		TestValue newValue = serializer.clone(initialValue);
		updater.update(newValue);
		assertEquals(initialValue, change.getOldValue());
		assertEquals(newValue, change.getNewValue());
		assertFalse(change.isDelete());
		assertFalse(change.isInsert());
		assertTrue(change.isUpdate());
		assertEquals(key, change.getKey());
	}

	@Test
	public void test_applyChange_insert() {
		BlueKey key = createKey(1, 2);
		removeKey(key);
		TestValue value = createValue("Joe");
		PendingChange<TestValue> change = PendingChange.createInsert(key, value, serializer);
		try {
			assertNull(COLLECTION.get(key));
			Segment<TestValue> segment = COLLECTION.getSegmentManager().getFirstSegment(key);
			change.applyChange(segment);
			assertEquals(value, COLLECTION.get(key));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		removeKey(key);
	}

	@Test
	public void test_applyChange_delete() {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		insert(key, value);
		PendingChange<TestValue> change = PendingChange.createDelete(key);
		try {
			assertEquals(value, COLLECTION.get(key));
			Segment<TestValue> segment = COLLECTION.getSegmentManager().getFirstSegment(key);
			change.applyChange(segment);
			assertNull(COLLECTION.get(key));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		removeKey(key);
	}

	@Test
	public void test_applyChange_update() {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		insert(key, value);
		Updater<TestValue> updater = ((v) -> v.addCupcake());
		TestValue newValue = serializer.clone(value);
		updater.update(newValue);
		PendingChange<TestValue> change = PendingChange.createUpdate(key, value, updater, serializer);
		try {
			assertEquals(value, COLLECTION.get(key));
			Segment<TestValue> segment = COLLECTION.getSegmentManager().getFirstSegment(key);
			change.applyChange(segment);
			assertEquals(newValue, COLLECTION.get(key));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		removeKey(key);
	}

	@Test
	public void test_DeleteMultipleTask_toString() {
		BlueKey key = createKey(1, 2);
		TestValue value = createValue("Joe");
		Updater<TestValue> updater = ((v) -> v.addCupcake());
		TestValue newValue = serializer.clone(value);
		PendingChange<TestValue> change = PendingChange.createUpdate(key, value, updater, serializer);
		String changeString = change.toString();
		assertTrue(changeString.contains(change.getClass().getSimpleName()));
		assertTrue(changeString.contains(key.toString()));
		assertTrue(changeString.contains(value.toString()));
		assertTrue(changeString.contains(newValue.toString()));
	}
	
	private TestValue createValue(String name){
		return new TestValue(name);
	}

	private BlueKey createKey(long keyId, long time){
		return new TimeKey(keyId, time);
	}

	private void insert(BlueKey key, TestValue value) {
		try {
			if (!COLLECTION.contains(key)) {
				COLLECTION.insert(key, value);
			}
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	private void removeKey(BlueKey key) {
		try {
			COLLECTION.delete(key);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}
}
