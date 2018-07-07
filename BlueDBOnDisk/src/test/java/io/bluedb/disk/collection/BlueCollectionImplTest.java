package io.bluedb.disk.collection;

import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;

import org.junit.Test;
import io.bluedb.api.BlueDb;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.StringKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.BlueDbOnDiskBuilder;
import io.bluedb.disk.TestValue;
import junit.framework.TestCase;

public class BlueCollectionImplTest extends TestCase {

	BlueDb db;
	BlueCollectionImpl<TestValue> collection;
	Path dbPath;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		dbPath = Paths.get("test_BlueCollectionImplTest");
		db = new BlueDbOnDiskBuilder().setPath(dbPath).build();
		collection = (BlueCollectionImpl<TestValue>) db.getCollection(TestValue.class, "testing");
		collection.query().delete();
	}

	@Override
	public void tearDown() throws Exception {
		Files.walk(dbPath)
		.sorted(Comparator.reverseOrder())
		.map(Path::toFile)
		.forEach(File::delete);

		collection.query().delete();
	}

	@Test
	public void test_query() {
		TestValue value = new TestValue("Joe");
		insert(1, value);
		try {
			List<TestValue> values = getCollection().query().getList();
			assertEquals(1, values.size());
			assertTrue(values.contains(value));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_contains() {
		TestValue value = new TestValue("Joe");
		insert(1, value);
		try {
			List<TestValue> values = getCollection().query().getList();
			assertEquals(1, values.size());
			assertTrue(values.contains(value));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_get() {
		TestValue value = new TestValue("Joe");
		TestValue differentValue = new TestValue("Bob");
		BlueKey key = createTimeKey(10, value);
		BlueKey sameTimeDifferentValue = createTimeKey(10, differentValue);
		BlueKey sameValueDifferentTime = createTimeKey(20, value);
		BlueKey differentValueAndTime = createTimeKey(20, differentValue);
		insert(key, value);
		try {
			assertEquals(value, getCollection().get(key));
			assertNotEquals(value, differentValue);
			assertNotEquals(value, getCollection().get(sameTimeDifferentValue));
			assertNotEquals(value, getCollection().get(sameValueDifferentTime));
			assertNotEquals(value, getCollection().get(differentValueAndTime));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_insert() {
		TestValue value = new TestValue("Joe");
		BlueKey key = createTimeKey(10, value);
		insert(key, value);
		assertValueAtKey(key, value);
		try {
			getCollection().insert(key, value); // insert duplicate
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_update() {
		BlueKey key = insert(10, new TestValue("Joe", 0));
		try {
			assertCupcakes(key, 0);
			getCollection().update(key, (v) -> v.addCupcake());
			assertCupcakes(key, 1);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_update_invalid() {
		TestValue value = new TestValue("Joe", 0);
		BlueKey key = insert(1, value);
		try {
			getCollection().update(key, (v) -> v.doSomethingNaughty());
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_delete() {
		TestValue value = new TestValue("Joe");
		BlueKey key = insert(10, value);
		try {
			assertValueAtKey(key, value);
			getCollection().delete(key);
			assertValueNotAtKey(key, value);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_findMatches() {
		// TODO
	}

	@Test
	public void test_applyChange() {
		// TODO
	}

	@Test
	public void test_executeTask() {
		// TODO
	}

	@Test
	public void test_rollup() {
		// TODO
	}

	@Test
	public void test_scheduleRollup() {
		// TODO
	}

	@Test
	public void test_updateAll_invalid() {
		TestValue value = new TestValue("Joe", 0);
		insert(1, value);
		try {
			getCollection().query().update((v) -> v.doSomethingNaughty());
			fail();
		} catch (BlueDbException e) {
		}
	}

	private void assertCupcakes(BlueKey key, int cupcakes) {
		try {
			TestValue value = getCollection().get(key);
			if (value == null)
				fail();
			assertEquals(cupcakes, value.getCupcakes());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	private void assertValueNotAtKey(BlueKey key, TestValue value) {
		try {
			assertNotEquals(value, getCollection().get(key));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	private void assertValueAtKey(BlueKey key, TestValue value) {
		TestValue differentValue = new TestValue("Bob");
		differentValue.setCupcakes(42);
		try {
			assertEquals(value, getCollection().get(key));
			assertNotEquals(value, differentValue);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	private BlueKey insert(long time, TestValue value) {
		BlueKey key = createTimeKey(time, value);
		insert(key, value);
		return key;
	}

	private void insert(BlueKey key, TestValue value) {
		try {
			getCollection().insert(key, value);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	private TimeKey createTimeFrameKey(long start, long end, TestValue obj) {
		StringKey stringKey = new StringKey(obj.getName());
		return new TimeFrameKey(stringKey, start, end);
	}

	private TimeKey createTimeKey(long time, TestValue obj) {
		StringKey stringKey = new StringKey(obj.getName());
		return new TimeKey(stringKey, time);
	}

	private BlueCollectionImpl<TestValue> getCollection() {
		return collection;
	}
}
