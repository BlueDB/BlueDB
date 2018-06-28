package io.bluedb.disk.collection;

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

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		db = new BlueDbOnDiskBuilder().build();
		collection = (BlueCollectionImpl<TestValue>) db.getCollection(TestValue.class, "testing");
		collection.query().delete();
	}

	@Override
	protected void tearDown() throws Exception {
		collection.query().delete();
	}

	@Test
	public void testQuery() {
		// TODO
	}

	@Test
	public void testContains() {
		// TODO
	}

	@Test
	public void testGet() {
		// TODO
	}

	@Test
	public void testInsert() {
		// TODO
	}

	@Test
	public void testUpdate() {
		// TODO
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
	public void testDelete() {
		// TODO
	}

	@Test
	public void testGetList() {
		// TODO
	}

	@Test
	public void test_updateAll_invalid() {
		TestValue value = new TestValue("Joe", 0);
		BlueKey key = insert(1, value);
		try {
			getCollection().query().update((v) -> v.doSomethingNaughty());
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void testDeleteAll() {
		// TODO
	}

	@Test
	public void testGetFirstSegment() {
		// TODO
	}

	@Test
	public void testGetSegments() {
		// TODO
	}

	@Test
	public void testGetRecoveryManager() {
		// TODO
	}

	@Test
	public void testGetPath() {
		// TODO
	}

	@Test
	public void testGetSerializer() {
		// TODO
	}

	@Test
	public void testShutdown() {
		// TODO
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
