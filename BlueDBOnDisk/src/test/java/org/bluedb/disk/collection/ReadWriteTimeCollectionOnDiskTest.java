package org.bluedb.disk.collection;

import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bluedb.TestUtils;
import org.bluedb.api.Condition;
import org.bluedb.api.ReadableBlueCollection;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.HashGroupedKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.BlueDbOnDiskBuilder;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.ReadWriteDbOnDisk;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.index.ReadableIndexOnDisk;
import org.bluedb.disk.collection.index.TestRetrievalKeyExtractor;
import org.bluedb.disk.models.calls.Call;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadWriteSegment;
import org.bluedb.disk.segment.ReadWriteSegmentManager;
import org.bluedb.disk.segment.rollup.RollupScheduler;
import org.bluedb.disk.segment.rollup.RollupTarget;
import org.bluedb.disk.serialization.BlueEntity;
import org.junit.Test;

public class ReadWriteTimeCollectionOnDiskTest extends BlueDbDiskTestBase {

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
	public void test_batchDelete() throws Exception {
		TestValue value1 = new TestValue("Joe");
		TestValue value2 = new TestValue("Bob");
		TestValue value3 = new TestValue("Chuck");
		BlueKey key1 = createTimeKey(10, value1);
		BlueKey key2 = createTimeKey(20, value2);
		BlueKey key3 = createTimeKey(20, value3);
		Map<BlueKey, TestValue> batchInserts = new HashMap<>();
		batchInserts.put(key1, value1);
		batchInserts.put(key2, value2);
		batchInserts.put(key3, value3);

		getTimeCollection().batchUpsert(batchInserts);
		assertValueAtKey(key1, value1);
		assertValueAtKey(key2, value2);
		assertValueAtKey(key3, value3);

		getTimeCollection().query()
			.whereKeyIsIn(new HashSet<>(Arrays.asList(key1, key3)))
			.delete();
		assertValueNotAtKey(key1, value1);
		assertValueAtKey(key2, value2);
		assertValueNotAtKey(key3, value3);
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

		assertEquals(listEmpty, getValuesByIndexForTargetIndexedInteger(getTimeCollection(), index, indexKeyFor1and3));
		getTimeCollection().batchUpsert(batchInserts);
		assertEquals(list1and3, getValuesByIndexForTargetIndexedInteger(getTimeCollection(), index, indexKeyFor1and3));
	}
	
	@Test
	public void test_batchUpsert_withIndex() throws BlueDbException {
		KeyExtractor<IntegerKey, TestValue> keyExtractor = new TestRetrievalKeyExtractor();
		ReadableIndexOnDisk<IntegerKey, TestValue> index = (ReadableIndexOnDisk<IntegerKey, TestValue>) getTimeCollection().createIndex("test_index", IntegerKey.class, keyExtractor);
		
		int indexValue1 = 42;
		int indexValue2 = 777;
		int indexValue3 = 9999999;
		int indexValue4 = 3117;
		
		TestValue value1 = new TestValue("Joe", indexValue1);
		BlueKey key1 = createTimeKey(10, value1);
		
		TestValue value2 = new TestValue("Bob", indexValue2);
		BlueKey key2 = createTimeKey(20, value2);
		
		TestValue value3 = new TestValue("Charlie", indexValue1);
		BlueKey key3 = createTimeKey(30, value3);

		Set<BlueKey> emptyKeySet = new HashSet<>();
		Set<BlueKey> keys1_3 = new HashSet<>(Arrays.asList(key1, key3));
		Set<BlueKey> keys2 = new HashSet<>(Arrays.asList(key2));
		
		List<TestValue> emptyList = Arrays.asList();
		List<TestValue> values1_2_3 = Arrays.asList(value1, value2, value3);
		List<TestValue> values1_3 = Arrays.asList(value1, value3);
		List<TestValue> values2 = Arrays.asList(value2);

		//Should start empty
		assertEquals(emptyList, getTimeCollection().query().getList());
		assertEquals(emptyKeySet, index.getKeys(new IntegerKey(indexValue1)));
		assertEquals(emptyKeySet, index.getKeys(new IntegerKey(indexValue2)));
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(getTimeCollection(), index, new IntegerKey(indexValue1)));
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(getTimeCollection(), index, new IntegerKey(indexValue2)));
		
		Map<BlueKey, TestValue> entriesToUpsert = new HashMap<>();
		entriesToUpsert.put(key1, value1);
		entriesToUpsert.put(key2, value2);
		entriesToUpsert.put(key3, value3);
		getTimeCollection().batchUpsert(entriesToUpsert);
		
		//Collection values, index keys, and looking up values by index key should all reflect the inserted items
		assertEquals(values1_2_3, getTimeCollection().query().getList());
		assertEquals(keys1_3, index.getKeys(new IntegerKey(indexValue1)));
		assertEquals(keys2, index.getKeys(new IntegerKey(indexValue2)));
		assertEquals(values1_3, getValuesByIndexForTargetIndexedInteger(getTimeCollection(), index, new IntegerKey(indexValue1)));
		assertEquals(values2, getValuesByIndexForTargetIndexedInteger(getTimeCollection(), index, new IntegerKey(indexValue2)));
		
		TestValue value4 = new TestValue("Aaron", indexValue1);
		BlueKey key4 = createTimeKey(5, value4);
		
		TestValue value5 = new TestValue("Rodger", indexValue3);
		BlueKey key5 = createTimeKey(1000000000, value5);
		
		TestValue newValue1 = value1.cloneWithNewCupcakeCount(indexValue4);
		TestValue newValue2 = value1.cloneWithNewCupcakeCount(indexValue4);
		
		entriesToUpsert = new HashMap<>();
		entriesToUpsert.put(key1, newValue1); //Updating cup cake count
		entriesToUpsert.put(key2, newValue2); //Update cup cake count
		entriesToUpsert.put(key4, value4); //Insert
		entriesToUpsert.put(key5, value5); //Insert
		getTimeCollection().batchUpsert(entriesToUpsert);
		
		Set<BlueKey> keys1_2 = new HashSet<>(Arrays.asList(key1, key2));
		Set<BlueKey> keys3_4 = new HashSet<>(Arrays.asList(key3, key4));
		Set<BlueKey> keys5 = new HashSet<>(Arrays.asList(key5));
		
		List<TestValue> values1_2_3_4_5 = Arrays.asList(value4, newValue1, newValue2, value3, value5);
		List<TestValue> values1_2 = Arrays.asList(newValue1, newValue2);
		List<TestValue> values3_4 = Arrays.asList(value4, value3);
		List<TestValue> values5 = Arrays.asList(value5);
		
		/*
		 * Collection values, index keys, and looking up values by index key should all reflect the inserted/updated items.
		 * In the past the index keys were not kept up to date properly on batch upserts and values had to be verified and
		 * removed before returning values by index key.
		 */
		assertEquals(values1_2_3_4_5, getTimeCollection().query().getList());
		assertEquals(keys3_4, index.getKeys(new IntegerKey(indexValue1)));
		assertEquals(emptyKeySet, index.getKeys(new IntegerKey(indexValue2)));
		assertEquals(keys5, index.getKeys(new IntegerKey(indexValue3)));
		assertEquals(keys1_2, index.getKeys(new IntegerKey(indexValue4)));
		assertEquals(values3_4, getValuesByIndexForTargetIndexedInteger(getTimeCollection(), index, new IntegerKey(indexValue1)));
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(getTimeCollection(), index, new IntegerKey(indexValue2)));
		assertEquals(values5, getValuesByIndexForTargetIndexedInteger(getTimeCollection(), index, new IntegerKey(indexValue3)));
		assertEquals(values1_2, getValuesByIndexForTargetIndexedInteger(getTimeCollection(), index, new IntegerKey(indexValue4)));
	}
	
	@Test
	public void test_batchUpsert_invalidKeyType() throws BlueDbException {
		TestValue value1 = new TestValue("Joe", 2);
		BlueKey key1 = createTimeKey(10, value1);
		
		TestValue value2 = new TestValue("Bob", 5);
		BlueKey key2 = new IntegerKey(15);
		
		TestValue value3 = new TestValue("Charlie", 7);
		BlueKey key3 = createTimeKey(30, value3);

		List<TestValue> emptyList = Arrays.asList();

		assertEquals(emptyList, getTimeCollection().query().getList());
		
		Map<BlueKey, TestValue> entriesToUpsert = new HashMap<>();
		entriesToUpsert.put(key1, value1);
		entriesToUpsert.put(key2, value2);
		entriesToUpsert.put(key3, value3);
		
		try {
			getTimeCollection().batchUpsert(entriesToUpsert);
			fail();
		} catch(BlueDbException e) {
			//expected since key2 is of a type that doesn't make sense for the collection
		}
		
		assertEquals(emptyList, getTimeCollection().query().getList());
	}

	@Test
	public void test_insert_times() throws Exception {
		ReadWriteTimeCollectionOnDisk<String> stringCollection = (ReadWriteTimeCollectionOnDisk<String>) db().getTimeCollectionBuilder("test_strings", TimeKey.class, String.class).build();
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
		turnOnObjectValidation();
		
		BlueEntity<Call> invalidCall = TestUtils.loadCorruptCall();
		
		try {
			getCallCollection().insert(invalidCall.getKey(), invalidCall.getValue());
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_update() throws Exception {
		BlueKey key = insertAtTime(10, new TestValue("Joe", 0));
		BlueKey key2 = insertAtTime(10, new TestValue("Bob", 0));
        assertCupcakes(key, 0);
        assertCupcakes(key2, 0);
        getTimeCollection().update(key, (v) -> v.addCupcake());
        assertCupcakes(key, 1);
        assertCupcakes(key2, 0);
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
	public void test_replace() throws Exception {
		BlueKey key = insertAtTime(10, new TestValue("Joe", 0));
        assertCupcakes(key, 0);
        getTimeCollection().replace(key, (v) -> new TestValue(v.getName(), v.getCupcakes() + 1));
        assertCupcakes(key, 1);
	}

	@Test
	public void test_replace_nonexisting() {
		AtomicBoolean wasUpdaterCalled = new AtomicBoolean(false);
		BlueKey key = new TimeKey(1, 1);
		try {
	        getTimeCollection().replace(key, (v) -> new TestValue(v.getName(), v.getCupcakes() + 1));
			fail();
		} catch (BlueDbException e) {
		}
		assertFalse(wasUpdaterCalled.get());
	}

	@Test
	public void test_replace_invalid() {
		TestValue value = new TestValue("Joe", 0);
		BlueKey key = insertAtTime(1, value);
		try {
			getTimeCollection().replace(key, (v) -> {throw new RuntimeException("no go");}) ;
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
		allEntities = getTimeCollection().findMatches(new Range(0, 3), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), false, Optional.empty());
		entitiesWithJoe = getTimeCollection().findMatches(new Range(0, 5), new ArrayList<>(), Arrays.asList(isJoe), new ArrayList<>(), false, Optional.empty());
		entities3to5 = getTimeCollection().findMatches(new Range(3, 5), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), false, Optional.empty());
		entities2to3 = getTimeCollection().findMatches(new Range(2, 3), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), false, Optional.empty());
		entities0to1 = getTimeCollection().findMatches(new Range(0, 1), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), false, Optional.empty());
		entities0to0 = getTimeCollection().findMatches(new Range(0, 0), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), false, Optional.empty());

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

		allEntities = getTimeCollection().findMatches(new Range(0, 3), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), true, Optional.empty());
		entities3to5 = getTimeCollection().findMatches(new Range(3, 5), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), true, Optional.empty());
		entities2to3 = getTimeCollection().findMatches(new Range(2, 3), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), true, Optional.empty());
		entities0to1 = getTimeCollection().findMatches(new Range(0, 1), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), true, Optional.empty());
		entities0to0 = getTimeCollection().findMatches(new Range(0, 0), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), true, Optional.empty());

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

		ReadWriteSegment<TestValue> segment = getTimeCollection().getSegmentManager().getSegment(key1At1.getGroupingNumber());
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
	public void test_rollup_scheduling_presegment() throws Exception {
		long segmentSize = getTimeCollection().getSegmentManager().getSegmentSize();
		long segmentStart = segmentSize * 2;
		BlueKey key = new TimeFrameKey(1, 1, segmentStart + 1);
		Range rollupRange = new Range(0, segmentStart - 1);
		long rollupDelay = segmentSize * 2;
		RollupTarget rollupTarget = new RollupTarget(segmentStart, rollupRange, rollupDelay);

		TestValue value = createValue("Anna");
		getTimeCollection().insert(key, value);
		long now = System.currentTimeMillis();

		RollupScheduler scheduler = getTimeCollection().getRollupScheduler();
		Map<RollupTarget, Long> rollupTimes = scheduler.getRollupTimes();
		long rollupTime = rollupTimes.get(rollupTarget);
		assertTrue(rollupTime > now + rollupDelay - 10_000);
		assertTrue(rollupTime < now + rollupDelay + 10_000);

		assertEquals(Arrays.asList(value), getTimeCollection().query().getList());
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
	public void test_replaceAll_invalid() {
		TestValue value = new TestValue("Joe", 0);
		insertAtTime(1, value);
		try {
			getTimeCollection().query().replace((v) -> {v.doSomethingNaughty(); return null; });
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_ensureCorrectKeyType() throws BlueDbException {
		ReadWriteTimeCollectionOnDisk<String> collectionWithTimeKeys = (ReadWriteTimeCollectionOnDisk<String>) db().getTimeCollectionBuilder("test_collection_TimeKey", TimeKey.class, String.class).build();
		ReadWriteCollectionOnDisk<String> collectionWithLongKeys = (ReadWriteCollectionOnDisk<String>) db().getCollectionBuilder("test_collection_LongKey", LongKey.class, String.class).build();

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


	@Test
	public void test_determineKeyType() throws BlueDbException {
		db().getTimeCollectionBuilder(getTimeCollectionName(), TimeKey.class, TestValue.class).build();  // regular instantiation approach

		ReadWriteDbOnDisk reopenedDatbase = (ReadWriteDbOnDisk) new BlueDbOnDiskBuilder().withPath(db().getPath()).build();  // reopen database without collections instantiated

		try {
			reopenedDatbase.getTimeCollectionBuilder(getTimeCollectionName(), HashGroupedKey.class, TestValue.class).build();  // try to open with the wrong key type
			fail();
		} catch (BlueDbException e) {
		}

		ReadWriteTimeCollectionOnDisk<?> collectionWithoutType = (ReadWriteTimeCollectionOnDisk<?>) reopenedDatbase.getTimeCollectionBuilder(getTimeCollectionName(), null, TestValue.class).build();  // open without specifying key type
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
		values = getTimeCollection().query().getList();
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
	
	@Test
	public void test_isTimeBased() {
		assertFalse(getIntCollection().isTimeBased());
		assertFalse(getLongCollection().isTimeBased());
		assertFalse(getHashGroupedCollection().isTimeBased());
		assertTrue(getCallCollection().isTimeBased());
		assertTrue(getTimeCollection().isTimeBased());
	}

	private List<TestValue> getValuesByIndexForTargetIndexedInteger(ReadableBlueCollection<TestValue> collection, BlueIndex<IntegerKey, TestValue> index, IntegerKey targetIntegerKey) throws BlueDbException {
		return collection.query()
				.where(index.createIntegerIndexCondition().isEqualTo(targetIntegerKey.getId()))
				.getList();
	}
}
