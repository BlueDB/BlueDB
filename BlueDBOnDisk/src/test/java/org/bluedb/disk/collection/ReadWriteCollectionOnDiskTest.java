package org.bluedb.disk.collection;

import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bluedb.TestUtils;
import org.bluedb.api.ReadableBlueCollection;
import org.bluedb.api.datastructures.BlueKeyValuePair;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.HashGroupedKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.BlueDbOnDiskBuilder;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.ReadableDbOnDisk;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.index.TestRetrievalKeyExtractor;
import org.bluedb.disk.models.calls.Call;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadWriteSegment;
import org.bluedb.disk.segment.ReadWriteSegmentManager;
import org.bluedb.disk.serialization.BlueEntity;
import org.junit.Test;

public class ReadWriteCollectionOnDiskTest extends BlueDbDiskTestBase {

	@Test
	public void test_query() throws Exception {
		TestValue value = new TestValue("Joe");
		insertAtLong(1, value);
		List<TestValue> values = getLongCollection().query().getList();
		assertEquals(1, values.size());
		assertTrue(values.contains(value));
	}

	@Test
	public void test_contains() throws Exception {
		TestValue value = new TestValue("Joe");
		BlueKey key = new LongKey(1);
		getLongCollection().insert(key, value);
		assertTrue(getLongCollection().contains(key));
	}

	@Test
	public void test_get() throws Exception {
		TestValue value = new TestValue("Joe");
		TestValue differentValue = new TestValue("Bob");
		BlueKey key = new LongKey(10);
		BlueKey differentKey = new LongKey(20);
		getLongCollection().insert(key, value);
		assertEquals(value, getLongCollection().get(key));
		assertNotEquals(value, differentValue);
		assertNotEquals(value, getLongCollection().get(differentKey));
	}

	@Test
	public void test_insert() {
		TestValue value = new TestValue("Joe");
		BlueKey key = createTimeKey(10, value);
		insertToTimeCollection(key, value);
		assertValueAtKey(key, value);
		try {
			getLongCollection().insert(key, value); // insert duplicate
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_batchInsert() throws Exception {
		TestValue value1 = new TestValue("Joe");
		TestValue value2 = new TestValue("Bob");
		BlueKey key1 = new LongKey(10);
		BlueKey key2 = new LongKey(20);
		Map<BlueKey, TestValue> batchInserts = new HashMap<>();
		batchInserts.put(key1, value1);
		batchInserts.put(key2, value2);
		getLongCollection().batchUpsert(batchInserts);
		assertEquals(value1, getLongCollection().get(key1));
		assertEquals(value2, getLongCollection().get(key2));
	}

	@Test
	public void test_batchInsertWithIterator() throws Exception {
		TestValue value1 = new TestValue("Joe");
		TestValue value2 = new TestValue("Bob");
		BlueKey key1 = new LongKey(10);
		BlueKey key2 = new LongKey(20);
		
		List<BlueKeyValuePair<TestValue>> batchInserts = new LinkedList<>();
		batchInserts.add(new BlueKeyValuePair<>(key1, value1));
		batchInserts.add(new BlueKeyValuePair<>(key2, value2));
		
		getLongCollection().batchUpsert(batchInserts.iterator());
		assertEquals(value1, getLongCollection().get(key1));
		assertEquals(value2, getLongCollection().get(key2));
		
		List<BlueKeyValuePair<TestValue>> batchUpdates = new LinkedList<>();
		value1.addCupcake();
		value2.addCupcake();
		batchUpdates.add(new BlueKeyValuePair<>(key1, value1));
		batchUpdates.add(new BlueKeyValuePair<>(key2, value2));
		getLongCollection().batchUpsert(batchUpdates.iterator());
		assertEquals(value1, getLongCollection().get(key1));
		assertEquals(value2, getLongCollection().get(key2));
		
		List<BlueKeyValuePair<TestValue>> emptyUpdatesList = new LinkedList<>();
		getLongCollection().batchUpsert(emptyUpdatesList.iterator());
		assertEquals(value1, getLongCollection().get(key1));
		assertEquals(value2, getLongCollection().get(key2));
	}

	@Test
	public void test_batchDelete() throws Exception {
		TestValue value1 = new TestValue("Joe");
		TestValue value2 = new TestValue("Bob");
		TestValue value3 = new TestValue("Chuck");
		BlueKey key1 = new LongKey(10);
		BlueKey key2 = new LongKey(20);
		BlueKey key3 = new LongKey(30);
		Map<BlueKey, TestValue> batchInserts = new HashMap<>();
		batchInserts.put(key1, value1);
		batchInserts.put(key2, value2);
		batchInserts.put(key3, value3);

		getLongCollection().batchUpsert(batchInserts);
        assertEquals(value1, getLongCollection().get(key1));
        assertEquals(value2, getLongCollection().get(key2));
        assertEquals(value3, getLongCollection().get(key3));

		getLongCollection().batchDelete(Arrays.asList(key1, key3));
        assertFalse(getLongCollection().contains(key1));
        assertEquals(value2, getLongCollection().get(key2));
        assertFalse(getLongCollection().contains(key3));
	}

	@Test
	public void test_batchInsert_index() throws Exception {
		KeyExtractor<IntegerKey, TestValue> keyExtractor = new TestRetrievalKeyExtractor();
		BlueIndex<IntegerKey, TestValue> index = getLongCollection().createIndex("test_index", IntegerKey.class, keyExtractor);
		TestValue value1 = new TestValue("Joe");
		TestValue value2 = new TestValue("Bob");
		TestValue value3 = new TestValue("Charlie");
		value1.setCupcakes(42);
		value2.setCupcakes(777);
		value3.setCupcakes(42);
		IntegerKey indexKeyFor1and3 = new IntegerKey(42);
		BlueKey key1 = new LongKey(10);
		BlueKey key2 = new LongKey(20);
		BlueKey key3 = new LongKey(30);
		Map<BlueKey, TestValue> batchInserts = new HashMap<>();
		batchInserts.put(key1, value1);
		batchInserts.put(key2, value2);
		batchInserts.put(key3, value3);

		List<TestValue> listEmpty = Arrays.asList();
		List<TestValue> list1and3 = Arrays.asList(value1, value3);

		assertEquals(listEmpty, index.get(indexKeyFor1and3));
		getLongCollection().batchUpsert(batchInserts);
		assertEquals(list1and3, index.get(indexKeyFor1and3));
	}

	@Test
	public void test_insert_longs() throws Exception {
		ReadWriteCollectionOnDisk<String> stringCollection = (ReadWriteCollectionOnDisk<String>) db().getCollectionBuilder("test_strings", LongKey.class, String.class).build();
		String value = "string";
		int n = 100;
		for (int i = 0; i < n; i++) {
			long id = new Random().nextLong();
			LongKey key = new LongKey(id);
			stringCollection.insert(key, value);
		}
		List<String> storedValues = stringCollection.query().getList();
		assertEquals(n, storedValues.size());
	}

	@Test
	public void test_insert_long_strings() throws Exception {
		ReadWriteCollectionOnDisk<String> stringCollection = (ReadWriteCollectionOnDisk<String>) db().getCollectionBuilder("test_strings", StringKey.class, String.class).build();
		String value = "string";
		int n = 100;
		for (int i = 0; i < n; i++) {
			String id = UUID.randomUUID().toString();
			StringKey key = new StringKey(id);
			stringCollection.insert(key, value);
		}
		List<String> storedValues = stringCollection.query().getList();
		assertEquals(n, storedValues.size());
	}

	@Test
	public void test_insert_invalid() throws BlueDbException, URISyntaxException, IOException {
		BlueEntity<Call> invalidCall = TestUtils.loadCorruptCall();
		
		try {
			getCallCollection().insert(invalidCall.getKey(), invalidCall.getValue());
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_update() throws Exception {
		BlueKey key = insertAtLong(10, new TestValue("Joe", 0));
		BlueKey key2 = insertAtLong(20, new TestValue("Bob", 0));
		assertEquals(0, getLongCollection().get(key).getCupcakes());
		assertEquals(0, getLongCollection().get(key2).getCupcakes());
        getLongCollection().update(key, (v) -> v.addCupcake());
		assertEquals(1, getLongCollection().get(key).getCupcakes());
		assertEquals(0, getLongCollection().get(key2).getCupcakes());
	}

	@Test
	public void test_update_nonexisting() {
		AtomicBoolean wasUpdaterCalled = new AtomicBoolean(false);
		BlueKey key = new LongKey(1);
		try {
			getLongCollection().update(key, (v) -> wasUpdaterCalled.set(true));
			fail();
		} catch (BlueDbException e) {
		}
		assertFalse(wasUpdaterCalled.get());
	}

	@Test
	public void test_update_invalid() {
		TestValue value = new TestValue("Joe", 0);
		BlueKey key = insertAtLong(1, value);
		try {
			getLongCollection().update(key, (v) -> v.doSomethingNaughty());
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_replace() throws Exception {
		BlueKey key = insertAtLong(10, new TestValue("Joe", 0));
		assertEquals(0, getLongCollection().get(key).getCupcakes());
        getLongCollection().replace(key, (v) -> new TestValue(v.getName(), v.getCupcakes() + 1));
		assertEquals(1, getLongCollection().get(key).getCupcakes());
	}

	@Test
	public void test_replace_nonexisting() {
		AtomicBoolean wasUpdaterCalled = new AtomicBoolean(false);
		BlueKey key = new LongKey(1);
		try {
	        getLongCollection().replace(key, (v) -> new TestValue(v.getName(), v.getCupcakes() + 1));
			fail();
		} catch (BlueDbException e) {
		}
		assertFalse(wasUpdaterCalled.get());
	}

	@Test
	public void test_replace_invalid() {
		TestValue value = new TestValue("Joe", 0);
		BlueKey key = insertAtLong(1, value);
		try {
			getLongCollection().replace(key, (v) -> {throw new RuntimeException("no go");}) ;
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_delete() throws Exception {
		TestValue value = new TestValue("Joe");
        BlueKey key = insertAtLong(10, value);
        assertEquals(value, getLongCollection().get(key));
        getLongCollection().delete(key);
        assertFalse(getLongCollection().contains(key));
	}

	@Test
	public void test_getLastKey() throws Exception {
		assertNull(getLongCollection().getLastKey());
		BlueKey key1 = insertAtLong(1, new TestValue("Joe"));
		assertEquals(key1, getLongCollection().getLastKey());
		BlueKey key3 = insertAtLong(3, new TestValue("Bob"));
		assertEquals(key3, getLongCollection().getLastKey());
		@SuppressWarnings("unused")
		BlueKey key2 = insertAtLong(2, new TestValue("Fred"));
		assertEquals(key3, getLongCollection().getLastKey());
	}

	@Test
	public void test_getLastKey_readonly() throws Exception {
        ReadableDbOnDisk readOnlyDb = (ReadableDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db().getPath()).buildReadOnly();
        ReadableBlueCollection<TestValue> collection = readOnlyDb.getCollection(getLongCollection().getPath().toFile().getName(), TestValue.class);

		assertNull(collection.getLastKey());
		BlueKey key1 = insertAtLong(1, new TestValue("Joe"));
		assertEquals(key1, collection.getLastKey());
		BlueKey key3 = insertAtLong(3, new TestValue("Bob"));
		assertEquals(key3, collection.getLastKey());
		@SuppressWarnings("unused")
		BlueKey key2 = insertAtLong(2, new TestValue("Fred"));
		assertEquals(key3, collection.getLastKey());
	}

	@Test
	public void test_executeTask() throws Exception {
		AtomicBoolean hasRun = new AtomicBoolean(false);
		Runnable task = new Runnable() {
			@Override
			public void run() {
				try {
					Thread.sleep(10);  // make sure this test waits for the task to be complete
					hasRun.set(true);
				} catch (InterruptedException e) {
					e.printStackTrace();
					fail();
				}
			}
		};

		getLongCollection().executeTask(task);
		assertTrue(hasRun.get());
	}

	@Test
	public void test_updateAll_invalid() {
		TestValue value = new TestValue("Joe", 0);
		insertAtLong(1, value);
		try {
			getLongCollection().query().update((v) -> v.doSomethingNaughty());
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_replaceAll_invalid() {
		TestValue value = new TestValue("Joe", 0);
		insertAtLong(1, value);
		try {
			getLongCollection().query().replace((v) -> {v.doSomethingNaughty(); return null; });
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_query_HashGroupedKey() throws Exception {
		TestValue value = new TestValue("Joe");
		insertAtId(UUID.randomUUID(), value);
		List<TestValue> values = getHashGroupedCollection().query().getList();
		assertEquals(1, values.size());
		assertTrue(values.contains(value));
	}

	@Test
	public void test_contains_HashGroupedKey() throws Exception {
		TestValue value = new TestValue("Joe");
		HashGroupedKey<?> key = insertAtId(UUID.randomUUID(), value);
		List<TestValue> values = getHashGroupedCollection().query().getList();
		assertEquals(1, values.size());
		assertTrue(getHashGroupedCollection().contains(key));
	}

	@Test
	public void test_get_HashGroupedKey() throws Exception {
		TestValue value = new TestValue("Joe");
		BlueKey key = insertAtLong(1, value);
		assertEquals(value, getLongCollection().get(key));
	}

	@Test
	public void test_query_LongKey() throws Exception {
		TestValue value = new TestValue("Joe");
		insertAtLong(1, value);
		List<TestValue> values = getLongCollection().query().getList();
		assertEquals(1, values.size());
		assertTrue(values.contains(value));
	}

	@Test
	public void test_contains_LongKey() throws Exception {
		TestValue value = new TestValue("Joe");
		LongKey key = insertAtLong(1L, value);
		assertTrue(getLongCollection().contains(key));
	}

	@Test
	public void test_get_LongKey() throws Exception {
		TestValue value = new TestValue("Joe");
		BlueKey key = insertAtLong(1, value);
		assertEquals(value, getLongCollection().get(key));
	}

	@Test
	public void test_rollup_ValueKey_invalid_size() throws Exception {
		long segmentSize = getHashGroupedCollection().getSegmentManager().getSegmentSize();
		Range offByOneSegmentTimeRange1 = new Range(0, segmentSize);
		Range offByOneSegmentTimeRange2 = new Range(1, segmentSize);
		Range entireFirstSegmentTimeRange = new Range(0, segmentSize -1);
		try {
			getHashGroupedCollection().rollup(offByOneSegmentTimeRange1);
			fail();
		} catch (BlueDbException e) {}
		try {
			getHashGroupedCollection().rollup(offByOneSegmentTimeRange2);
			fail();
		} catch (BlueDbException e) {}
		getHashGroupedCollection().rollup(entireFirstSegmentTimeRange);
	}

	@Test
	public void test_rollup_ValueKey() throws Exception {
		BlueKey key0 = new IntegerKey(0);
		BlueKey key3 = new IntegerKey(3);
		TestValue value1 = createValue("Anna");
		TestValue value3 = createValue("Chuck");
		List<TestValue> values;
		values = getLongCollection().query().getList();
		assertEquals(0, values.size());

		getIntCollection().insert(key0, value1);
		getIntCollection().insert(key3, value3);
		values = getIntCollection().query().getList();
		assertEquals(2, values.size());

		ReadWriteSegmentManager<TestValue> segmentManager = getIntCollection().getSegmentManager();
		ReadWriteSegment<TestValue> segmentFor1 = segmentManager.getSegment(key0.getGroupingNumber());
		ReadWriteSegment<TestValue> segmentFor3 = segmentManager.getSegment(key3.getGroupingNumber());
		assertEquals(segmentFor1, segmentFor3);  // make sure they're in the same segment

		File[] segmentDirectoryContents = segmentFor1.getPath().toFile().listFiles();
		assertEquals(2, segmentDirectoryContents.length);

		long segmentSize = getIntCollection().getSegmentManager().getSegmentSize();
		long segmentStart = Blutils.roundDownToMultiple(key0.getGroupingNumber(), segmentSize);
		Range entireFirstSegmentTimeRange = new Range(segmentStart, segmentStart + segmentSize -1);
		Range offByOneSegmentTimeRange = new Range(segmentStart, segmentStart + segmentSize);
		try {
			getIntCollection().rollup(offByOneSegmentTimeRange);
			fail();
		} catch (BlueDbException e) {}

		getIntCollection().rollup(entireFirstSegmentTimeRange);

		values = getIntCollection().query().getList();
		assertEquals(2, values.size());
		segmentDirectoryContents = segmentFor1.getPath().toFile().listFiles();
		assertEquals(1, segmentDirectoryContents.length);

	}
}
