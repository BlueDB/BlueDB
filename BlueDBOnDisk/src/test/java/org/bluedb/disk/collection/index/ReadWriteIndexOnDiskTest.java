package org.bluedb.disk.collection.index;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.ReadableBlueCollection;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.BlueDbOnDiskBuilder;
import org.bluedb.disk.ReadableDbOnDisk;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.ReadWriteTimeCollectionOnDisk;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadWriteSegment;
import org.bluedb.disk.segment.rollup.IndexRollupTarget;
import org.bluedb.disk.segment.rollup.RollupScheduler;
import org.bluedb.disk.segment.rollup.RollupTarget;
import org.junit.Test;

public class ReadWriteIndexOnDiskTest extends BlueDbDiskTestBase {

	@Test
	public void test_getKeys() throws Exception {
		ReadWriteTimeCollectionOnDisk<TestValue> collection = getTimeCollection();
		BlueIndex<IntegerKey, TestValue> index = collection.createIndex("test_index", IntegerKey.class, new TestRetrievalKeyExtractor());
		ReadWriteIndexOnDisk<IntegerKey, TestValue> indexOnDisk = (ReadWriteIndexOnDisk<IntegerKey, TestValue>) index;

		TestValue valueFred1 = new TestValue("Fred", 1);
		TestValue valueBob3 = new TestValue("Bob", 3);
		TestValue valueJoe3 = new TestValue("Joe", 3);
		TimeKey timeKeyFred1 = createTimeKey(1, valueFred1);
		TimeKey timeKeyBob3 = createTimeKey(2, valueBob3);
		TimeKey timeKeyJoe3 = createTimeKey(3, valueJoe3);

		IntegerKey integerKey1 = new IntegerKey(1);
		IntegerKey integerKey2 = new IntegerKey(2);
		IntegerKey integerKey3 = new IntegerKey(3);

		List<BlueKey> emptyList = Arrays.asList();
		List<BlueKey> bobAndJoe = Arrays.asList(timeKeyBob3, timeKeyJoe3);
		List<BlueKey> justBob = Arrays.asList(timeKeyBob3);
		List<BlueKey> justFred = Arrays.asList(timeKeyFred1);

		assertEquals(emptyList, indexOnDisk.getKeys(integerKey1));
		assertEquals(emptyList, indexOnDisk.getKeys(integerKey2));
		assertEquals(emptyList, indexOnDisk.getKeys(integerKey3));

		collection.insert(timeKeyFred1, valueFred1);
		collection.insert(timeKeyBob3, valueBob3);
		collection.insert(timeKeyJoe3, valueJoe3);

		assertEquals(justFred, indexOnDisk.getKeys(integerKey1));
		assertEquals(emptyList, indexOnDisk.getKeys(integerKey2));
		assertEquals(bobAndJoe, indexOnDisk.getKeys(integerKey3));

		collection.delete(timeKeyFred1);
		collection.delete(timeKeyJoe3);

		assertEquals(emptyList, indexOnDisk.getKeys(integerKey1));
		assertEquals(emptyList, indexOnDisk.getKeys(integerKey2));
		assertEquals(justBob, indexOnDisk.getKeys(integerKey3));
	}

	@Test
	public void test_getKeys_multi() throws Exception {
		ReadWriteTimeCollectionOnDisk<TestValue> collection = getTimeCollection();
		BlueIndex<IntegerKey, TestValue> index = collection.createIndex("test_index", IntegerKey.class, new TestMultiRetrievalKeyExtractor());
		ReadWriteIndexOnDisk<IntegerKey, TestValue> indexOnDisk = (ReadWriteIndexOnDisk<IntegerKey, TestValue>) index;

		TestValue valueFred1 = new TestValue("Fred", 1);
		TimeKey timeKeyFred1 = createTimeKey(1, valueFred1);

		IntegerKey integerKey1 = new IntegerKey(1);
		IntegerKey integerKey2 = new IntegerKey(2);
		IntegerKey integerKey3 = new IntegerKey(3);

		List<BlueKey> emptyList = Arrays.asList();
		List<BlueKey> justFred = Arrays.asList(timeKeyFred1);

		assertEquals(emptyList, indexOnDisk.getKeys(integerKey1));
		assertEquals(emptyList, indexOnDisk.getKeys(integerKey2));
		assertEquals(emptyList, indexOnDisk.getKeys(integerKey3));

		collection.insert(timeKeyFred1, valueFred1);

		assertEquals(justFred, indexOnDisk.getKeys(integerKey1));
		assertEquals(emptyList, indexOnDisk.getKeys(integerKey2));
		assertEquals(justFred, indexOnDisk.getKeys(integerKey3));

		collection.delete(timeKeyFred1);

		assertEquals(emptyList, indexOnDisk.getKeys(integerKey1));
		assertEquals(emptyList, indexOnDisk.getKeys(integerKey2));
		assertEquals(emptyList, indexOnDisk.getKeys(integerKey3));
	}

	@Test
	public void test_get() throws Exception {
		ReadWriteTimeCollectionOnDisk<TestValue> collection = getTimeCollection();
		BlueIndex<IntegerKey, TestValue> index = collection.createIndex("test_index", IntegerKey.class, new TestRetrievalKeyExtractor());
		ReadWriteIndexOnDisk<IntegerKey, TestValue> indexOnDisk = (ReadWriteIndexOnDisk<IntegerKey, TestValue>) index;

		TestValue valueFred1 = new TestValue("Fred", 1);
		TestValue valueBob3 = new TestValue("Bob", 3);
		TestValue valueJoe3 = new TestValue("Joe", 3);
		TimeKey timeKeyFred1 = createTimeKey(1, valueFred1);
		TimeKey timeKeyBob3 = createTimeKey(2, valueBob3);
		TimeKey timeKeyJoe3 = createTimeKey(3, valueJoe3);

		IntegerKey integerKey1 = new IntegerKey(1);
		IntegerKey integerKey2 = new IntegerKey(2);
		IntegerKey integerKey3 = new IntegerKey(3);
		IntegerKey integerKey4 = new IntegerKey(4);

		List<TestValue> emptyList = Arrays.asList();
		List<TestValue> bobAndJoe = Arrays.asList(valueBob3, valueJoe3);
		List<TestValue> justBob = Arrays.asList(valueBob3);
		List<TestValue> justFred = Arrays.asList(valueFred1);

		assertEquals(emptyList, indexOnDisk.get(integerKey1));
		assertEquals(emptyList, indexOnDisk.get(integerKey2));
		assertEquals(emptyList, indexOnDisk.get(integerKey3));

		collection.insert(timeKeyFred1, valueFred1);
		collection.insert(timeKeyBob3, valueBob3);
		collection.insert(timeKeyJoe3, valueJoe3);

		assertEquals(justFred, indexOnDisk.get(integerKey1));
		assertEquals(emptyList, indexOnDisk.get(integerKey2));
		assertEquals(bobAndJoe, indexOnDisk.get(integerKey3));

		collection.delete(timeKeyFred1);
		collection.delete(timeKeyJoe3);

		assertEquals(emptyList, indexOnDisk.get(integerKey1));
		assertEquals(emptyList, indexOnDisk.get(integerKey2));
		assertEquals(justBob, indexOnDisk.get(integerKey3));
		
		
		collection.update(timeKeyBob3, value -> value.addCupcake());
		collection.getIndexManager().addToAllIndexes(timeKeyBob3, valueBob3); //Should make index 3 point to this value even though the indexed value was just changed to 4
		valueBob3.addCupcake();
		
		assertEquals(emptyList, indexOnDisk.get(integerKey3)); //3 should point to the value, but since the value doesn't contain 3 it shouldn't return it.
		assertEquals(justBob, indexOnDisk.get(integerKey4));
	}
	
	@Test
	public void test_indexKeyExtractorReturningNull() throws BlueDbException {
		ReadWriteTimeCollectionOnDisk<TestValue> collection = getTimeCollection();
		
		BlueIndex<IntegerKey, TestValue> index = collection.createIndex("test_index", IntegerKey.class, new NullReturningKeyExtractor());
		
		TestValue valueFred1 = new TestValue("Fred", 1);
		TimeKey timeKeyFred1 = createTimeKey(1, valueFred1);
		collection.getIndexManager().addToAllIndexes(timeKeyFred1, valueFred1);
		List<TestValue> emptyList = Arrays.asList();
		
		assertEquals(emptyList, index.get(new IntegerKey(1)));
	}

	@Test
	public void test_getIndex_readonly_wrongType() throws Exception {
		getTimeCollection().createIndex("test_index", IntegerKey.class, new TestRetrievalKeyExtractor());
		ReadableDbOnDisk readOnlyDb = (ReadableDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db().getPath()).buildReadOnly();
		ReadableBlueCollection<TestValue> readOnlyCollection = readOnlyDb.getTimeCollection(getTimeCollectionName(), TestValue.class);
		try {
			readOnlyCollection.getIndex("test_index", StringKey.class);
			fail();
		} catch (BlueDbException e) {}
	}

	@Test
	public void test_getIndex_readonly_nonExisting() throws Exception {
		TestValue value = new TestValue("Joe", 3);
		TimeKey key = createTimeKey(1, value);
		TestRetrievalKeyExtractor keyExtractor = new TestRetrievalKeyExtractor();
		IntegerKey indexKey = keyExtractor.extractKeys(value).get(0);
		String indexName = "test_index";

		ReadableDbOnDisk readOnlyDb = (ReadableDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db().getPath()).buildReadOnly();
		ReadableBlueCollection<TestValue> readOnlyCollection = readOnlyDb.getTimeCollection(getTimeCollectionName(), TestValue.class);
		BlueIndex<IntegerKey, TestValue> facadeIndex = readOnlyCollection.getIndex(indexName, IntegerKey.class);

		assertNull(facadeIndex.get(indexKey));
		assertNull(facadeIndex.getLastKey());

		getTimeCollection().createIndex(indexName, IntegerKey.class, keyExtractor);
		getTimeCollection().insert(key, value);

		assertEquals(Arrays.asList(value), facadeIndex.get(indexKey));
		assertEquals(indexKey, facadeIndex.getLastKey());
	}


	@Test
	public void test_getIndex_nonExisting() throws Exception {
		BlueCollection<TestValue> readOnlyCollection = db().getTimeCollection(getTimeCollectionName(), TestValue.class);
		try {
			readOnlyCollection.getIndex("test_index", StringKey.class);
			fail();
		} catch (BlueDbException e) {}
	}

	@Test
	public void test_get_readonly() throws Exception {
		ReadWriteTimeCollectionOnDisk<TestValue> collection = getTimeCollection();
		collection.createIndex("test_index", IntegerKey.class, new TestRetrievalKeyExtractor());

		ReadableDbOnDisk readOnlyDb = (ReadableDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db().getPath()).buildReadOnly();
		ReadableBlueCollection<TestValue> readOnlyCollection = readOnlyDb.getTimeCollection(getTimeCollectionName(), TestValue.class);
		BlueIndex<IntegerKey, TestValue> readOnlyIndex = readOnlyCollection.getIndex("test_index", IntegerKey.class);

		TestValue valueFred1 = new TestValue("Fred", 1);
		TestValue valueBob3 = new TestValue("Bob", 3);
		TestValue valueJoe3 = new TestValue("Joe", 3);
		TimeKey timeKeyFred1 = createTimeKey(1, valueFred1);
		TimeKey timeKeyBob3 = createTimeKey(2, valueBob3);
		TimeKey timeKeyJoe3 = createTimeKey(3, valueJoe3);

		IntegerKey integerKey1 = new IntegerKey(1);
		IntegerKey integerKey2 = new IntegerKey(2);
		IntegerKey integerKey3 = new IntegerKey(3);

		List<TestValue> emptyList = Arrays.asList();
		List<TestValue> bobAndJoe = Arrays.asList(valueBob3, valueJoe3);
		List<TestValue> justBob = Arrays.asList(valueBob3);
		List<TestValue> justFred = Arrays.asList(valueFred1);

		assertEquals(emptyList, readOnlyIndex.get(integerKey1));
		assertEquals(emptyList, readOnlyIndex.get(integerKey2));
		assertEquals(emptyList, readOnlyIndex.get(integerKey3));

		collection.insert(timeKeyFred1, valueFred1);
		collection.insert(timeKeyBob3, valueBob3);
		collection.insert(timeKeyJoe3, valueJoe3);

		assertEquals(justFred, readOnlyIndex.get(integerKey1));
		assertEquals(emptyList, readOnlyIndex.get(integerKey2));
		assertEquals(bobAndJoe, readOnlyIndex.get(integerKey3));

		collection.delete(timeKeyFred1);
		collection.delete(timeKeyJoe3);

		assertEquals(emptyList, readOnlyIndex.get(integerKey1));
		assertEquals(emptyList, readOnlyIndex.get(integerKey2));
		assertEquals(justBob, readOnlyIndex.get(integerKey3));
	}

	@Test
	public void test_createNew_populateNewIndex() throws Exception {
		ReadWriteTimeCollectionOnDisk<TestValue> collection = getTimeCollection();

		TestValue valueFred1 = new TestValue("Fred", 1);
		TestValue valueBob3 = new TestValue("Bob", 3);
		TestValue valueJoe3 = new TestValue("Joe", 3);
		TimeKey timeKeyFred1 = createTimeKey(1, valueFred1);
		TimeKey timeKeyBob3 = createTimeKey(2, valueBob3);
		TimeKey timeKeyJoe3 = createTimeKey(3, valueJoe3);

		IntegerKey integerKey1 = new IntegerKey(1);
		IntegerKey integerKey2 = new IntegerKey(2);
		IntegerKey integerKey3 = new IntegerKey(3);

		List<TestValue> emptyList = Arrays.asList();
		List<TestValue> bobAndJoe = Arrays.asList(valueBob3, valueJoe3);
		List<TestValue> justBob = Arrays.asList(valueBob3);
		List<TestValue> justFred = Arrays.asList(valueFred1);

		collection.insert(timeKeyFred1, valueFred1);
		collection.insert(timeKeyBob3, valueBob3);
		collection.insert(timeKeyJoe3, valueJoe3);

		BlueIndex<IntegerKey, TestValue> index = collection.createIndex("test_index", IntegerKey.class, new TestRetrievalKeyExtractor());
		ReadWriteIndexOnDisk<IntegerKey, TestValue> indexOnDisk = (ReadWriteIndexOnDisk<IntegerKey, TestValue>) index;

		assertEquals(justFred, indexOnDisk.get(integerKey1));
		assertEquals(emptyList, indexOnDisk.get(integerKey2));
		assertEquals(bobAndJoe, indexOnDisk.get(integerKey3));

		collection.delete(timeKeyFred1);
		collection.delete(timeKeyJoe3);

		assertEquals(emptyList, indexOnDisk.get(integerKey1));
		assertEquals(emptyList, indexOnDisk.get(integerKey2));
		assertEquals(justBob, indexOnDisk.get(integerKey3));
	}

	@Test
	public void test_rollup() throws Exception {
		TestRetrievalKeyExtractor keyExtractor = new TestRetrievalKeyExtractor();
		ReadWriteTimeCollectionOnDisk<TestValue> collection = getTimeCollection();
		BlueIndex<IntegerKey, TestValue> index = collection.createIndex("test_index", IntegerKey.class, keyExtractor);
		ReadWriteIndexOnDisk<IntegerKey, TestValue> indexOnDisk = (ReadWriteIndexOnDisk<IntegerKey, TestValue>) index;

		BlueKey key1At1 = createKey(1, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna", 1);
		TestValue value3 = createValue("Chuck", 3);
		List<TestValue> values;

		values = collection.query().getList();
		assertEquals(0, values.size());

		collection.insert(key1At1, value1);
		collection.insert(key3At3, value3);
		values = collection.query().getList();
		assertEquals(2, values.size());

		BlueKey retrievalKey1 = keyExtractor.extractKeys(value1).get(0);
		ReadWriteSegment<?> indexSegment = indexOnDisk.getSegmentManager().getSegment(retrievalKey1.getGroupingNumber());
		File segmentFolder = indexSegment.getPath().toFile();
		File[] segmentDirectoryContents = segmentFolder.listFiles();
		assertEquals(2, segmentDirectoryContents.length);

		
		Range entireFirstSegmentTimeRange = indexSegment.getRange();
		Range offByOneSegmentTimeRange = new Range(entireFirstSegmentTimeRange.getStart(), entireFirstSegmentTimeRange.getEnd() + 1);
		try {
			indexOnDisk.rollup(offByOneSegmentTimeRange);
			fail();
		} catch (BlueDbException e) {}
		try {
			indexOnDisk.rollup(entireFirstSegmentTimeRange);
		} catch (BlueDbException e) {
			fail();
		}

		values = collection.query().getList();
		assertEquals(2, values.size());
		segmentDirectoryContents = segmentFolder.listFiles();
		assertEquals(1, segmentDirectoryContents.length);
	}

	@Test
	public void test_rollup_scheduling() throws Exception {
		String indexName = "test_index";
		TestRetrievalKeyExtractor keyExtractor = new TestRetrievalKeyExtractor();
		ReadWriteTimeCollectionOnDisk<TestValue> collection = getTimeCollection();
		collection.createIndex(indexName, IntegerKey.class, keyExtractor);

		BlueKey key1At1 = createKey(1, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna", 1);
		TestValue value3 = createValue("Chuck", 3);
		List<TestValue> values;
		IntegerKey indexKey = keyExtractor.extractKeys(value1).get(0);
		long groupingNumber = indexKey.getGroupingNumber();
		long startOfRange = groupingNumber - (groupingNumber % 256);
		Range rollupRange = new Range(startOfRange, startOfRange + 256 - 1);

		Map<RollupTarget, Long> rollupTimes;
		RollupScheduler scheduler = getTimeCollection().getRollupScheduler();
		IndexRollupTarget target_256 = new IndexRollupTarget(indexName, rollupRange.getStart(), rollupRange);

		rollupTimes = scheduler.getRollupTimes();
		assertEquals(0, rollupTimes.size());

		values = getTimeCollection().query().getList();
		assertEquals(0, values.size());

		getTimeCollection().insert(key1At1, value1);
		getTimeCollection().insert(key3At3, value3);
		values = getTimeCollection().query().getList();
		assertEquals(2, values.size());

		rollupTimes = scheduler.getRollupTimes();
		assertTrue(rollupTimes.containsKey(target_256));
	}

	@Test
	public void test_getLastKey() throws Exception {
		ReadWriteTimeCollectionOnDisk<TestValue> collection = getTimeCollection();
		BlueIndex<IntegerKey, TestValue> index = collection.createIndex("test_index", IntegerKey.class, new TestRetrievalKeyExtractor());
//		BlueIndexOnDisk<IntegerKey, TestValue> indexOnDisk = (BlueIndexOnDisk<IntegerKey, TestValue>) index;

		IntegerKey integerKey1 = new IntegerKey(1);
//		IntegerKey integerKey2 = new IntegerKey(2);
		IntegerKey integerKey3 = new IntegerKey(3);

		TestValue value1 = new TestValue("Fred", 1);
		TestValue value2 = new TestValue("Bob", 2);
		TestValue value3 = new TestValue("Joe", 3);
		TestValue value3B = new TestValue("Sally", 3);

		// make sure it's null if collection empty
		assertNull(index.getLastKey());

		// make sure it updates on first insert
		insertAtTime(4, value1);
		assertEquals(integerKey1, index.getLastKey());

		// make sure it updates on additional insert
		insertAtTime(5, value3);
		assertEquals(integerKey3, index.getLastKey());

		// make sure it doesn't go backwards
		insertAtTime(6, value2);
		assertEquals(integerKey3, index.getLastKey());

		// make sure duplicates at the max don't break anything
		insertAtTime(7, value3B);
		assertEquals(integerKey3, index.getLastKey());
	}
}
