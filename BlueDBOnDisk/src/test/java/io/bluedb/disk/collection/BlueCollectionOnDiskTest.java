package io.bluedb.disk.collection;

import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import io.bluedb.api.BlueCollection;
import io.bluedb.api.Condition;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.index.BlueIndex;
import io.bluedb.api.index.KeyExtractor;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.HashGroupedKey;
import io.bluedb.api.keys.IntegerKey;
import io.bluedb.api.keys.LongKey;
import io.bluedb.api.keys.StringKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.BlueDbOnDisk;
import io.bluedb.disk.BlueDbOnDiskBuilder;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.collection.index.TestRetrievalKeyExtractor;
import io.bluedb.disk.segment.Segment;
import io.bluedb.disk.segment.SegmentManager;
import io.bluedb.disk.segment.rollup.RollupScheduler;
import io.bluedb.disk.segment.rollup.RollupTarget;
import io.bluedb.disk.segment.Range;
import io.bluedb.disk.serialization.BlueEntity;

public class BlueCollectionOnDiskTest extends BlueDbDiskTestBase {

	@Test
	public void test_query() throws Exception {
		TestValue value = new TestValue("Joe");
		insertAtTime(1, value);
		List<TestValue> values = getTimeCollection().query().getList();
		assertEquals(1, values.size());
		assertTrue(values.contains(value));
	}

	@Test
	public void test_contains() throws Exception {
		TestValue value = new TestValue("Joe");
		BlueKey key = createTimeKey(1, value);
		getTimeCollection().insert(key, value);
		assertTrue(getTimeCollection().contains(key));
	}

	@Test
	public void test_get() throws Exception {
		TestValue value = new TestValue("Joe");
		TestValue differentValue = new TestValue("Bob");
		BlueKey key = createTimeKey(10, value);
		BlueKey sameTimeDifferentValue = createTimeKey(10, differentValue);
		BlueKey sameValueDifferentTime = createTimeKey(20, value);
		BlueKey differentValueAndTime = createTimeKey(20, differentValue);
		insertToTimeCollection(key, value);
		assertEquals(value, getTimeCollection().get(key));
		assertNotEquals(value, differentValue);
		assertNotEquals(value, getTimeCollection().get(sameTimeDifferentValue));
		assertNotEquals(value, getTimeCollection().get(sameValueDifferentTime));
		assertNotEquals(value, getTimeCollection().get(differentValueAndTime));
	}

	@Test
	public void test_insert() {
		TestValue value = new TestValue("Joe");
		BlueKey key = createTimeKey(10, value);
		insertToTimeCollection(key, value);
		assertValueAtKey(key, value);
		try {
			getTimeCollection().insert(key, value); // insert duplicate
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_batchInsert() throws Exception {
		TestValue value1 = new TestValue("Joe");
		TestValue value2 = new TestValue("Bob");
		BlueKey key1 = createTimeKey(10, value1);
		BlueKey key2 = createTimeKey(20, value2);
		Map<BlueKey, TestValue> batchInserts = new HashMap<>();
		batchInserts.put(key1, value1);
		batchInserts.put(key2, value2);
		getTimeCollection().batchUpsert(batchInserts);
		assertValueAtKey(key1, value1);
		assertValueAtKey(key2, value2);
	}

	@Test
	public void test_batchInsert_spanningSegments() throws Exception {
		long segmentSize = getTimeCollection().getSegmentManager().getSegmentSize();
		TestValue value = new TestValue("Joe");
		BlueKey key = createTimeFrameKey(segmentSize - 1, segmentSize + 1, value);
		Map<BlueKey, TestValue> batchInserts = new HashMap<>();
		batchInserts.put(key, value);
		getTimeCollection().batchUpsert(batchInserts);
		assertValueAtKey(key, value);
	}

	@Test
	public void test_batchInsert_index() throws Exception {
		KeyExtractor<IntegerKey, TestValue> keyExtractor = new TestRetrievalKeyExtractor();
		BlueIndex<IntegerKey, TestValue> index = getTimeCollection().createIndex("test_index", IntegerKey.class, keyExtractor);
		TestValue value1 = new TestValue("Joe");
		TestValue value2 = new TestValue("Bob");
		TestValue value3 = new TestValue("Charlie");
		value1.setCupcakes(42);
		value2.setCupcakes(777);
		value3.setCupcakes(42);
		IntegerKey indexKeyFor1and3 = new IntegerKey(42);
		BlueKey key1 = createTimeKey(10, value1);
		BlueKey key2 = createTimeKey(20, value2);
		BlueKey key3 = createTimeKey(30, value3);
		Map<BlueKey, TestValue> batchInserts = new HashMap<>();
		batchInserts.put(key1, value1);
		batchInserts.put(key2, value2);
		batchInserts.put(key3, value3);

		List<TestValue> listEmpty = Arrays.asList();
		List<TestValue> list1and3 = Arrays.asList(value1, value3);

		assertEquals(listEmpty, index.get(indexKeyFor1and3));
		getTimeCollection().batchUpsert(batchInserts);
		assertEquals(list1and3, index.get(indexKeyFor1and3));
	}

	@Test
	public void test_insert_times() throws Exception {
		@SuppressWarnings("unchecked")
		BlueCollectionOnDisk<String> stringCollection = (BlueCollectionOnDisk<String>) db().initializeCollection("test_strings", TimeKey.class, String.class);
		String value = "string";
		int n = 100;
		for (int i = 0; i < n; i++) {
			TimeKey key = new TimeKey(i, i);
			stringCollection.insert(key, value);
		}
		List<String> storedValues = stringCollection.query().getList();
		assertEquals(n, storedValues.size());
	}

	@Test
	public void test_insert_longs() throws Exception {
		@SuppressWarnings("unchecked")
		BlueCollectionOnDisk<String> stringCollection = (BlueCollectionOnDisk<String>) db().initializeCollection("test_strings", LongKey.class, String.class);
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
		@SuppressWarnings("unchecked")
		BlueCollectionOnDisk<String> stringCollection = (BlueCollectionOnDisk<String>) db().initializeCollection("test_strings", StringKey.class, String.class);
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
	public void test_update() throws Exception {
		BlueKey key = insertAtTime(10, new TestValue("Joe", 0));
        assertCupcakes(key, 0);
        getTimeCollection().update(key, (v) -> v.addCupcake());
        assertCupcakes(key, 1);
	}

	@Test
	public void test_update_nonexisting() {
		AtomicBoolean wasUpdaterCalled = new AtomicBoolean(false);
		BlueKey key = new TimeKey(1, 1);
		try {
			getTimeCollection().update(key, (v) -> wasUpdaterCalled.set(true));
			fail();
		} catch (BlueDbException e) {
		}
		assertFalse(wasUpdaterCalled.get());
	}

	@Test
	public void test_update_invalid() {
		TestValue value = new TestValue("Joe", 0);
		BlueKey key = insertAtTime(1, value);
		try {
			getTimeCollection().update(key, (v) -> v.doSomethingNaughty());
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_delete() throws Exception {
		TestValue value = new TestValue("Joe");
        BlueKey key = insertAtTime(10, value);
        assertValueAtKey(key, value);
        getTimeCollection().delete(key);
        assertValueNotAtKey(key, value);
	}

	@Test
	public void test_getLastKey() throws Exception {
		assertNull(getTimeCollection().getLastKey());
		BlueKey key1 = insertAtTime(1, new TestValue("Joe"));
		assertEquals(key1, getTimeCollection().getLastKey());
		BlueKey key3 = insertAtTime(3, new TestValue("Bob"));
		assertEquals(key3, getTimeCollection().getLastKey());
		@SuppressWarnings("unused")
		BlueKey key2 = insertAtTime(2, new TestValue("Fred"));
		assertEquals(key3, getTimeCollection().getLastKey());
	}

	@Test
	public void test_findMatches() throws Exception {
		TestValue valueJoe = new TestValue("Joe");
		TestValue valueBob = new TestValue("Bob");
		insertAtTime(1, valueJoe);
		insertAtTime(2, valueBob);
		List<BlueEntity<TestValue>> allEntities, entitiesWithJoe, entities3to5, entities2to3, entities0to1, entities0to0;

		Condition<TestValue> isJoe = (v) -> v.getName().equals("Joe");
		allEntities = getTimeCollection().findMatches(new Range(0, 3), new ArrayList<>(), false);
		entitiesWithJoe = getTimeCollection().findMatches(new Range(0, 5), Arrays.asList(isJoe), false);
		entities3to5 = getTimeCollection().findMatches(new Range(3, 5), new ArrayList<>(), false);
		entities2to3 = getTimeCollection().findMatches(new Range(2, 3), new ArrayList<>(), false);
		entities0to1 = getTimeCollection().findMatches(new Range(0, 1), new ArrayList<>(), false);
		entities0to0 = getTimeCollection().findMatches(new Range(0, 0), new ArrayList<>(), false);

		assertEquals(2, allEntities.size());
		assertEquals(1, entitiesWithJoe.size());
		assertEquals(valueJoe, entitiesWithJoe.get(0).getValue());
		assertEquals(0, entities3to5.size());
		assertEquals(1, entities2to3.size());
		assertEquals(valueBob, entities2to3.get(0).getValue());
		assertEquals(1, entities0to1.size());
		assertEquals(valueJoe, entities0to1.get(0).getValue());
		assertEquals(0, entities0to0.size());
	}

	@Test
	public void test_findMatches_byStartTime() throws Exception {
		TestValue valueJoe = new TestValue("Joe");
		TestValue valueBob = new TestValue("Bob");
		insertAtTimeFrame(1, 2, valueJoe);
		insertAtTimeFrame(2, 3, valueBob);
		List<BlueEntity<TestValue>> allEntities, entities3to5, entities2to3, entities0to1, entities0to0;

		allEntities = getTimeCollection().findMatches(new Range(0, 3), new ArrayList<>(), true);
		entities3to5 = getTimeCollection().findMatches(new Range(3, 5), new ArrayList<>(), true);
		entities2to3 = getTimeCollection().findMatches(new Range(2, 3), new ArrayList<>(), true);
		entities0to1 = getTimeCollection().findMatches(new Range(0, 1), new ArrayList<>(), true);
		entities0to0 = getTimeCollection().findMatches(new Range(0, 0), new ArrayList<>(), true);

		assertEquals(2, allEntities.size());
		assertEquals(0, entities3to5.size());
		assertEquals(1, entities2to3.size());
		assertEquals(valueBob, entities2to3.get(0).getValue());
		assertEquals(1, entities0to1.size());
		assertEquals(valueJoe, entities0to1.get(0).getValue());
		assertEquals(0, entities0to0.size());
	}

	@Test
	public void test_count_byStartTime() throws Exception {
		TestValue valueJoe = new TestValue("Joe");
		TestValue valueBob = new TestValue("Bob");
		insertAtTimeFrame(1, 2, valueJoe);
		insertAtTimeFrame(2, 3, valueBob);

		assertEquals(2, getTimeCollection().query().byStartTime().afterOrAtTime(1).beforeOrAtTime(3).count());
		assertEquals(2, getTimeCollection().query().byStartTime().afterOrAtTime(1).beforeOrAtTime(2).count());
		assertEquals(1, getTimeCollection().query().byStartTime().afterOrAtTime(2).beforeOrAtTime(3).count());
		assertEquals(0, getTimeCollection().query().byStartTime().afterOrAtTime(3).beforeOrAtTime(4).count());
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

		getTimeCollection().executeTask(task);
		assertTrue(hasRun.get());
	}

	@Test
	public void test_rollup() throws Exception {
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value3 = createValue("Chuck");
		List<TestValue> values;

		values = getTimeCollection().query().getList();
		assertEquals(0, values.size());

		getTimeCollection().insert(key1At1, value1);
		getTimeCollection().insert(key3At3, value3);
		values = getTimeCollection().query().getList();
		assertEquals(2, values.size());

		Segment<TestValue> segment = getTimeCollection().getSegmentManager().getSegment(key1At1.getGroupingNumber());
		File[] segmentDirectoryContents = segment.getPath().toFile().listFiles();
		assertEquals(2, segmentDirectoryContents.length);

		long segmentSize = getTimeCollection().getSegmentManager().getSegmentSize();
		Range offByOneSegmentTimeRange = new Range(0, segmentSize);
		Range entireFirstSegmentTimeRange = new Range(0, segmentSize -1);
		try {
			getTimeCollection().rollup(offByOneSegmentTimeRange);
			fail();
		} catch (BlueDbException e) {}
		try {
			getTimeCollection().rollup(entireFirstSegmentTimeRange);
		} catch (BlueDbException e) {
			fail();
		}

		values = getTimeCollection().query().getList();
		assertEquals(2, values.size());
		segmentDirectoryContents = segment.getPath().toFile().listFiles();
		assertEquals(1, segmentDirectoryContents.length);
	}

	@Test
	public void test_rollup_scheduling() throws Exception {
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value3 = createValue("Chuck");
		List<TestValue> values;

		Map<RollupTarget, Long> rollupTimes;
		RollupScheduler scheduler = getTimeCollection().getRollupScheduler();
		RollupTarget target_6000 = new RollupTarget(0, new Range(0, 5999));
		RollupTarget target_3600000 = new RollupTarget(0, new Range(0, 3599999));
		Set<RollupTarget> targets_none = new HashSet<>();
		Set<RollupTarget> targets_mid_and_top = new HashSet<>(Arrays.asList(target_6000, target_3600000));
//		Set<RollupTarget> targets_top = new HashSet<>(Arrays.asList(target_3600000));
		
		rollupTimes = scheduler.getRollupTimes();
		assertEquals(targets_none, rollupTimes.keySet());

		values = getTimeCollection().query().getList();
		assertEquals(0, values.size());

		getTimeCollection().insert(key1At1, value1);
		getTimeCollection().insert(key3At3, value3);
		values = getTimeCollection().query().getList();
		assertEquals(2, values.size());

		rollupTimes = scheduler.getRollupTimes();
		assertEquals(targets_mid_and_top, rollupTimes.keySet());
		assertTrue(rollupTimes.get(target_3600000) > rollupTimes.get(target_6000));
	}

	@Test
	public void test_updateAll_invalid() {
		TestValue value = new TestValue("Joe", 0);
		insertAtTime(1, value);
		try {
			getTimeCollection().query().update((v) -> v.doSomethingNaughty());
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_ensureCorrectKeyType() throws BlueDbException {
		@SuppressWarnings("unchecked")
		BlueCollection<?> collectionWithTimeKeys =db().initializeCollection("test_collection_TimeKey", TimeKey.class, Serializable.class);
		@SuppressWarnings("unchecked")
		BlueCollection<?> collectionWithLongKeys = db().initializeCollection("test_collection_LongKey", LongKey.class, Serializable.class);
		collectionWithTimeKeys.get(new TimeKey(1, 1));  // should not throw an Exception
		collectionWithTimeKeys.get(new TimeFrameKey(1, 1, 1));  // should not throw an Exception
		collectionWithLongKeys.get(new LongKey(1));  // should not throw an Exception
		try {
			collectionWithTimeKeys.get(new LongKey(1));
			fail();
		} catch (BlueDbException e){}
		try {
			collectionWithLongKeys.get(new TimeKey(1, 1));
			fail();
		} catch (BlueDbException e){}
	}


	@SuppressWarnings("unchecked")
	@Test
	public void test_determineKeyType() throws BlueDbException {
		db().initializeCollection(getTimeCollectionName(), TimeKey.class, TestValue.class);  // regular instantiation approach

		BlueDbOnDisk reopenedDatbase = new BlueDbOnDiskBuilder().setPath(db().getPath()).build();  // reopen database without collections instantiated

		try {
			reopenedDatbase.initializeCollection(getTimeCollectionName(), HashGroupedKey.class, TestValue.class);  // try to open with the wrong key type
			fail();
		} catch (BlueDbException e) {
		}

		BlueCollectionOnDisk<?> collectionWithoutType = (BlueCollectionOnDisk<?>) reopenedDatbase.initializeCollection(getTimeCollectionName(), null, TestValue.class);  // open without specifying key type
		assertEquals(TimeKey.class, collectionWithoutType.getKeyType());
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
		HashGroupedKey key = insertAtId(UUID.randomUUID(), value);
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
		values = getTimeCollection().query().getList();
		assertEquals(0, values.size());

		getIntCollection().insert(key0, value1);
		getIntCollection().insert(key3, value3);
		values = getIntCollection().query().getList();
		assertEquals(2, values.size());

		SegmentManager<TestValue> segmentManager = getIntCollection().getSegmentManager();
		Segment<TestValue> segmentFor1 = segmentManager.getSegment(key0.getGroupingNumber());
		Segment<TestValue> segmentFor3 = segmentManager.getSegment(key3.getGroupingNumber());
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
