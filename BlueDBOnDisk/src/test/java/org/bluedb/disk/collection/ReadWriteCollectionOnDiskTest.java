package org.bluedb.disk.collection;

import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bluedb.TestUtils;
import org.bluedb.api.BlueCollectionVersion;
import org.bluedb.api.BlueTimeCollection;
import org.bluedb.api.CloseableIterator;
import org.bluedb.api.Condition;
import org.bluedb.api.ReadableBlueCollection;
import org.bluedb.api.datastructures.BlueKeyValuePair;
import org.bluedb.api.datastructures.BlueSimpleSet;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.index.conditions.BlueIndexCondition;
import org.bluedb.api.keys.ActiveTimeKey;
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
import org.bluedb.disk.ReadableDbOnDisk;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.index.TestRetrievalKeyExtractor;
import org.bluedb.disk.collection.index.TestRetrievalLongKeyExtractor;
import org.bluedb.disk.collection.index.conditions.dummy.DummyUUIDIndexCondition;
import org.bluedb.disk.collection.metadata.ReadOnlyCollectionMetadata;
import org.bluedb.disk.models.calls.Call;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadWriteSegment;
import org.bluedb.disk.segment.ReadWriteSegmentManager;
import org.bluedb.disk.segment.SegmentSizeSetting;
import org.bluedb.disk.serialization.BlueEntity;
import org.junit.Test;
import org.mockito.Mockito;

public class ReadWriteCollectionOnDiskTest extends BlueDbDiskTestBase {
	
	@Test
	public void test_getSegmentSizeSettings() throws BlueDbException {
		assertEquals(SegmentSizeSetting.getDefaultSettingsFor(TimeKey.class).getConfig(), getTimeCollection().getSegmentSizeSettings().getConfig());
	}
	
	@Test
	public void test_getVersion() throws BlueDbException {
		assertEquals(BlueCollectionVersion.getDefault(), getTimeCollection().getVersion());
	}
	
	@Test
	public void test_determineCollectionVersion() throws BlueDbException {
		
		ArrayList<ReadOnlyCollectionMetadata> metaDataReturningValidVersions = new ArrayList<>();
		for(BlueCollectionVersion version : BlueCollectionVersion.values()) {
			ReadOnlyCollectionMetadata versionReturningMetadata = Mockito.mock(ReadOnlyCollectionMetadata.class);
			Mockito.doReturn(version).when(versionReturningMetadata).getCollectionVersion();
			metaDataReturningValidVersions.add(versionReturningMetadata);
		}
		
		ArrayList<Boolean> booleanOptions = new ArrayList<>(Arrays.asList(Boolean.TRUE, Boolean.FALSE));
		
		for(ReadOnlyCollectionMetadata metaDataReturningValidVersion : metaDataReturningValidVersions) {
			for(BlueCollectionVersion requestedVersion : BlueCollectionVersion.values()) {
				for(Boolean isNewCollection : booleanOptions) {
					assertEquals("If the meta data returns a current collection version then that is what we should always use", 
							metaDataReturningValidVersion.getCollectionVersion(), ReadableCollectionOnDisk.determineCollectionVersion(metaDataReturningValidVersion, requestedVersion, isNewCollection));
				}
			}
		}
		
		ReadOnlyCollectionMetadata nullReturningMetadata = Mockito.mock(ReadOnlyCollectionMetadata.class);
		Mockito.doReturn(null).when(nullReturningMetadata).getCollectionVersion();
		
		for(BlueCollectionVersion requestedVersion : BlueCollectionVersion.values()) {
			assertEquals("If there is no current collection version and it is not a new collection then that means this is a legacy version 1 collection from before we saved the version", 
					BlueCollectionVersion.VERSION_1, ReadableCollectionOnDisk.determineCollectionVersion(nullReturningMetadata, requestedVersion, false));
		}
		
		for(BlueCollectionVersion requestedVersion : BlueCollectionVersion.values()) {
			assertEquals("If there is no current collection version and it is a new collection then we will use the requested version", 
					requestedVersion, ReadableCollectionOnDisk.determineCollectionVersion(nullReturningMetadata, requestedVersion, true));
		}
		
		assertEquals("If there is no current collection version and it is a new collection and there is no requested version then we will return the default version", 
				BlueCollectionVersion.getDefault(), ReadableCollectionOnDisk.determineCollectionVersion(nullReturningMetadata, null, true));
	}
	
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
        
		getLongCollection().query()
			.whereKeyIsIn(new HashSet<>(Arrays.asList(key1, key3)))
			.delete();
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

		assertEquals(listEmpty, getValuesByIndexForTargetIndexedInteger(getLongCollection(), index, indexKeyFor1and3));
		getLongCollection().batchUpsert(batchInserts);
		assertEquals(list1and3, getValuesByIndexForTargetIndexedInteger(getLongCollection(), index, indexKeyFor1and3));
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
	public void test_getLastValue() throws Exception {
		assertNull(getLongCollection().getLastValue());
		insertAtLong(1, new TestValue("Joe"));
		assertEquals(new TestValue("Joe"), getLongCollection().getLastValue());
		insertAtLong(3, new TestValue("Bob"));
		assertEquals(new TestValue("Bob"), getLongCollection().getLastValue());
		insertAtLong(2, new TestValue("Fred"));
		assertEquals(new TestValue("Bob"), getLongCollection().getLastValue());
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
	public void test_getLastValue_readonly() throws Exception {
		ReadableDbOnDisk readOnlyDb = (ReadableDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db().getPath()).buildReadOnly();
		ReadableBlueCollection<TestValue> collection = readOnlyDb.getCollection(getLongCollection().getPath().toFile().getName(), TestValue.class);

		assertNull(collection.getLastValue());
		insertAtLong(1, new TestValue("Joe"));
		assertEquals(new TestValue("Joe"), collection.getLastValue());
		insertAtLong(3, new TestValue("Bob"));
		assertEquals(new TestValue("Bob"), collection.getLastValue());
		insertAtLong(2, new TestValue("Fred"));
		assertEquals(new TestValue("Bob"), collection.getLastValue());
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
	public void test_isIndexConditionCompatible() throws BlueDbException {
		BlueIndexCondition<TestValue> anonomysInnerIndexConditionDefinition = new BlueIndexCondition<TestValue>() {
			@Override public BlueIndexCondition<TestValue> isEqualTo(TestValue value) { return this; }
			@Override public BlueIndexCondition<TestValue> isIn(Set<TestValue> values) { return this; }
			@Override public BlueIndexCondition<TestValue> isIn(BlueSimpleSet<TestValue> values) { return this; }
			@Override public BlueIndexCondition<TestValue> meets(Condition<TestValue> condition) { return this; }
			@Override public CloseableIterator<BlueKey> getMatchingValueKeysIterator() { return null; }
		};
		assertFalse("You have to use an index condition that is an instance of OnDiskIndexCondition", getTimeCollection().isCompatibleIndexCondition(anonomysInnerIndexConditionDefinition));
		
		DummyUUIDIndexCondition<Serializable> dummyIndexConditionOfWrongType = new DummyUUIDIndexCondition<Serializable>(Serializable.class);
		assertFalse("You have to use an index for the same value type as the collection", getTimeCollection().isCompatibleIndexCondition(dummyIndexConditionOfWrongType));
		
		DummyUUIDIndexCondition<TestValue> dummyIndexCondition = new DummyUUIDIndexCondition<TestValue>(TestValue.class);
		assertTrue("Dummy indices don't return anything, so I'm not too worried aobut checking all of the details for those", getTimeCollection().isCompatibleIndexCondition(dummyIndexCondition));
		
		BlueIndex<IntegerKey, TestValue> index1OnTimeCollection = getTimeCollection().createIndex("index-1", IntegerKey.class, new TestRetrievalKeyExtractor());
		BlueIndex<LongKey, TestValue> index2OnTimeCollection = getTimeCollection().createIndex("index-2", LongKey.class, new TestRetrievalLongKeyExtractor());
		BlueIndex<IntegerKey, TestValue> index3OnTimeCollection = getTimeCollection().createIndex("index-3", IntegerKey.class, new TestRetrievalKeyExtractor());
		BlueIndex<IntegerKey, TestValue> index1OnIntCollection = getIntCollection().createIndex("index-1", IntegerKey.class, new TestRetrievalKeyExtractor());
		
		assertTrue(getTimeCollection().isCompatibleIndexCondition(index1OnTimeCollection.createIntegerIndexCondition()));
		assertTrue(getTimeCollection().isCompatibleIndexCondition(index2OnTimeCollection.createLongIndexCondition()));
		assertTrue(getIntCollection().isCompatibleIndexCondition(index1OnIntCollection.createIntegerIndexCondition()));
		
		assertFalse("Wrong collection", getIntCollection().isCompatibleIndexCondition(index1OnTimeCollection.createIntegerIndexCondition()));
		assertFalse("Wrong collection", getIntCollection().isCompatibleIndexCondition(index2OnTimeCollection.createLongIndexCondition()));
		assertFalse("Wrong collection", getTimeCollection().isCompatibleIndexCondition(index1OnIntCollection.createIntegerIndexCondition()));
		
		assertFalse("Index name and type doesn't exist on this collection", getIntCollection().isCompatibleIndexCondition(index3OnTimeCollection.createIntegerIndexCondition()));
		
		try {
			getTimeCollection().query().where(index1OnIntCollection.createIntegerIndexCondition());
			fail("Trying to use an invalid index condition in a where clause should have resulted in an exception being thrown.");
		} catch(InvalidParameterException e) { }
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

	private List<TestValue> getValuesByIndexForTargetIndexedInteger(ReadableBlueCollection<TestValue> collection, BlueIndex<IntegerKey, TestValue> index, IntegerKey targetIntegerKey) throws BlueDbException {
		return collection.query()
				.where(index.createIntegerIndexCondition().isEqualTo(targetIntegerKey.getId()))
				.getList();
	}
	
	@Test
	public void test_query_whereKeyIsIn_version1IncludesPreSegmentRange() throws Exception {
		/*
		 * The where key is in method for version 1 collections needs to include pre-segment keys
		 * in the first segment included range info.
		 */
		BlueTimeCollection<TestValue> timeCollection = db().getTimeCollectionBuilder("my-time-collection", TimeKey.class, TestValue.class)
			.withCollectionVersion(BlueCollectionVersion.VERSION_1)
			.build();
		
		long oneHour = TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
		long now = System.currentTimeMillis();
		long fourHoursAgo = now - oneHour*4;
		long fiveHoursAgo = now - oneHour*5;
		long sevenHoursAgo = now - oneHour*7;
		long eightHoursAgo = now - oneHour*8;

		ArrayList<BlueKey> keys = new ArrayList<>();
		keys.add(new TimeFrameKey(0, eightHoursAgo + 10, fourHoursAgo));
		keys.add(new TimeFrameKey(1, sevenHoursAgo, now));
		
		Map<BlueKey, TestValue> keyValuePairs = new HashMap<>();
		for(int i = 0; i < keys.size(); i++) {
			keyValuePairs.put(keys.get(i), new TestValue(String.valueOf(i), i));
		}
		
		timeCollection.batchUpsert(keyValuePairs);
		
		List<TestValue> queryResults = timeCollection.query()
				.whereKeyIsIn(new HashSet<>(Arrays.asList(keys.get(0), keys.get(1))))
				.afterOrAtTime(fiveHoursAgo)
				.beforeOrAtTime(now)
				.getList();
		assertQueryResults(queryResults, 0, 0, 1);
	}
	
	@Test
	public void test_query_IncludeAll_OnlyActive_OrOnlyCompleted() throws BlueDbException {
		/*
		 * The where key is in method for version 1 collections needs to include pre-segment keys
		 * in the first segment included range info.
		 */
		BlueTimeCollection<TestValue> timeCollection = db().getTimeCollectionBuilder("my-time-collection", TimeKey.class, TestValue.class)
			.withCollectionVersion(BlueCollectionVersion.VERSION_2)
			.build();
		
		long oneHour = TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
		long now = System.currentTimeMillis();
		long threeHoursAgo = now - oneHour*3;
		long fourHoursAgo = now - oneHour*4;
		long fiveHoursAgo = now - oneHour*5;
		long sevenHoursAgo = now - oneHour*7;
		long eightHoursAgo = now - oneHour*8;

		ArrayList<BlueKey> keys = new ArrayList<>();
		keys.add(new TimeFrameKey(0, eightHoursAgo + 10, fourHoursAgo));
		keys.add(new TimeFrameKey(1, sevenHoursAgo, now));
		keys.add(new ActiveTimeKey(2, threeHoursAgo));
		
		Map<BlueKey, TestValue> keyValuePairs = new HashMap<>();
		for(int i = 0; i < keys.size(); i++) {
			keyValuePairs.put(keys.get(i), new TestValue(String.valueOf(i), i));
		}
		
		timeCollection.batchUpsert(keyValuePairs);
		
		List<TestValue> queryResults = timeCollection.query()
				.afterOrAtTime(fiveHoursAgo)
				.beforeOrAtTime(now)
				.getList();
		assertQueryResults(queryResults, 0, 0, 1, 2);
		
		queryResults = timeCollection.query()
				.afterOrAtTime(fiveHoursAgo)
				.beforeOrAtTime(now)
				.whereKeyIsActive()
				.getList();
		assertQueryResults(queryResults, 0, 2);
		
		queryResults = timeCollection.query()
				.afterOrAtTime(fiveHoursAgo)
				.beforeOrAtTime(now)
				.whereKeyIsNotActive()
				.getList();
		assertQueryResults(queryResults, 0, 0, 1);
	}

	private void assertQueryResults(List<TestValue> actualValues, int cupcakeModifier, int...expectedValueIndicies) {
		List<TestValue> expectedValues = new LinkedList<>();
		for(int i = 0; i < expectedValueIndicies.length; i++) {
			int expectedIndex = expectedValueIndicies[i];
			expectedValues.add(new TestValue(String.valueOf(expectedIndex), expectedIndex + cupcakeModifier));
		}
		assertEquals(expectedValues, actualValues);
	}
}
