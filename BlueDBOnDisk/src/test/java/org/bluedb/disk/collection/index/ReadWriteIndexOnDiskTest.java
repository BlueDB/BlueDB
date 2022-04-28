package org.bluedb.disk.collection.index;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.ReadableBlueCollection;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.exceptions.UnsupportedIndexConditionTypeException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.BlueIndexInfo;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.api.keys.UUIDKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.BlueDbOnDiskBuilder;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.ReadableDbOnDisk;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.ReadWriteTimeCollectionOnDisk;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadWriteSegment;
import org.bluedb.disk.segment.rollup.IndexRollupTarget;
import org.bluedb.disk.segment.rollup.RollupScheduler;
import org.bluedb.disk.segment.rollup.RollupTarget;
import org.bluedb.disk.serialization.BlueEntity;
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

		Set<BlueKey> emptyList = new HashSet<>(Arrays.asList());
		Set<BlueKey> bobAndJoe = new HashSet<>(Arrays.asList(timeKeyBob3, timeKeyJoe3));
		Set<BlueKey> justBob = new HashSet<>(Arrays.asList(timeKeyBob3));
		Set<BlueKey> justFred = new HashSet<>(Arrays.asList(timeKeyFred1));

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

		Set<BlueKey> emptySet = new HashSet<>(Arrays.asList());
		Set<BlueKey> justFred = new HashSet<>(Arrays.asList(timeKeyFred1));

		assertEquals(emptySet, indexOnDisk.getKeys(integerKey1));
		assertEquals(emptySet, indexOnDisk.getKeys(integerKey2));
		assertEquals(emptySet, indexOnDisk.getKeys(integerKey3));

		collection.insert(timeKeyFred1, valueFred1);

		assertEquals(justFred, indexOnDisk.getKeys(integerKey1));
		assertEquals(emptySet, indexOnDisk.getKeys(integerKey2));
		assertEquals(justFred, indexOnDisk.getKeys(integerKey3));

		collection.delete(timeKeyFred1);

		assertEquals(emptySet, indexOnDisk.getKeys(integerKey1));
		assertEquals(emptySet, indexOnDisk.getKeys(integerKey2));
		assertEquals(emptySet, indexOnDisk.getKeys(integerKey3));
	}
	
	@Test
	public void test_extractIndexKeys() throws BlueDbException {

		TestValue valueFred1 = new TestValue("Fred", 1);
		TimeKey timeKeyFred1 = createTimeKey(1, valueFred1);
		BlueEntity<TestValue> fredEntity = new BlueEntity<>(timeKeyFred1, valueFred1);
		
		ReadWriteTimeCollectionOnDisk<TestValue> collection = getTimeCollection();
		ReadWriteIndexOnDisk<IntegerKey, TestValue> indexOnDisk = (ReadWriteIndexOnDisk<IntegerKey, TestValue>) collection.createIndex("test_index", IntegerKey.class, new TestRetrievalKeyExtractor());
		assertEquals(Arrays.asList(new IntegerKey(valueFred1.getCupcakes())), indexOnDisk.extractIndexKeys(fredEntity));
		
		ReadWriteIndexOnDisk<IntegerKey, TestValue> nullExtractingIndexOnDisk = (ReadWriteIndexOnDisk<IntegerKey, TestValue>) collection.createIndex("test_index2", IntegerKey.class, new TestRetrievalKeyNullExtractor());
		assertEquals(Arrays.asList(), nullExtractingIndexOnDisk.extractIndexKeys(fredEntity));
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

		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey1));
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey2));
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey3));

		collection.insert(timeKeyFred1, valueFred1);
		collection.insert(timeKeyBob3, valueBob3);
		collection.insert(timeKeyJoe3, valueJoe3);

		assertEquals(justFred, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey1));
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey2));
		assertEquals(bobAndJoe, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey3));

		collection.delete(timeKeyFred1);
		collection.delete(timeKeyJoe3);

		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey1));
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey2));
		assertEquals(justBob, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey3));
		
		
		collection.update(timeKeyBob3, value -> value.addCupcake());
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey1));
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey2));
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey3));
		
		/*
		 * The collection on disk has 4 cupcakes for bob, we're going to deliberately mess up the index
		 * by making the index think that it has 3 in it. Getting the list of values with 3 cupcakes
		 * should filter Bob out since he actually has 4. 
		 */
		collection.getIndexManager().indexChange(timeKeyBob3, null, valueBob3); //Should make index 3 point to this value even though the indexed value was just changed to 4
		valueBob3.addCupcake();
		
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey3)); //3 should point to the value, but since the value doesn't contain 3 it shouldn't return it.
		assertEquals(justBob, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey4));
	}
	
	@Test
	public void test_indexKeyExtractorReturningNull() throws BlueDbException {
		ReadWriteTimeCollectionOnDisk<TestValue> collection = getTimeCollection();
		
		BlueIndex<IntegerKey, TestValue> index = collection.createIndex("test_index", IntegerKey.class, new NullReturningKeyExtractor());
		
		TestValue valueFred1 = new TestValue("Fred", 1);
		TimeKey timeKeyFred1 = createTimeKey(1, valueFred1);
		collection.getIndexManager().indexChange(timeKeyFred1, null, valueFred1);
		List<TestValue> emptyList = Arrays.asList();
		
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, index, new IntegerKey(1)));
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

		readOnlyCollection.query()
		.where(facadeIndex.createIntegerIndexCondition().isEqualTo(indexKey.getId()))
		.getList();
		
		assertEquals(0, readOnlyCollection.query()
				.where(facadeIndex.createIntegerIndexCondition().isEqualTo(indexKey.getId()))
				.getList()
				.size());
		assertEquals(0, readOnlyCollection.query()
				.where(facadeIndex.createLongIndexCondition().isEqualTo(10L))
				.getList()
				.size());
		assertEquals(0, readOnlyCollection.query()
				.where(facadeIndex.createStringIndexCondition().isEqualTo("Anything"))
				.getList()
				.size());
		assertEquals(0, readOnlyCollection.query()
				.where(facadeIndex.createUUIDIndexCondition().isEqualTo(UUID.randomUUID()))
				.getList()
				.size());
		assertNull(facadeIndex.getLastKey());

		getTimeCollection().createIndex(indexName, IntegerKey.class, keyExtractor);
		getTimeCollection().insert(key, value);

		assertEquals(Arrays.asList(value), getValuesByIndexForTargetIndexedInteger(readOnlyCollection, facadeIndex, indexKey));
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

		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(readOnlyCollection, readOnlyIndex, integerKey1));
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(readOnlyCollection, readOnlyIndex, integerKey2));
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(readOnlyCollection, readOnlyIndex, integerKey3));

		collection.insert(timeKeyFred1, valueFred1);
		collection.insert(timeKeyBob3, valueBob3);
		collection.insert(timeKeyJoe3, valueJoe3);

		assertEquals(justFred, getValuesByIndexForTargetIndexedInteger(readOnlyCollection, readOnlyIndex, integerKey1));
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(readOnlyCollection, readOnlyIndex, integerKey2));
		assertEquals(bobAndJoe, getValuesByIndexForTargetIndexedInteger(readOnlyCollection, readOnlyIndex, integerKey3));

		collection.delete(timeKeyFred1);
		collection.delete(timeKeyJoe3);

		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(readOnlyCollection, readOnlyIndex, integerKey1));
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(readOnlyCollection, readOnlyIndex, integerKey2));
		assertEquals(justBob, getValuesByIndexForTargetIndexedInteger(readOnlyCollection, readOnlyIndex, integerKey3));
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

		assertEquals(justFred, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey1));
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey2));
		assertEquals(bobAndJoe, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey3));

		collection.delete(timeKeyFred1);
		collection.delete(timeKeyJoe3);

		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey1));
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey2));
		assertEquals(justBob, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey3));
	}

	@Test
	public void test_createNew_populateNewIndices() throws Exception {
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
		List<TestValue> fredBobAndJoe = Arrays.asList(valueFred1, valueBob3, valueJoe3);
		List<TestValue> justBob = Arrays.asList(valueBob3);
		List<TestValue> justFred = Arrays.asList(valueFred1);

		collection.insert(timeKeyFred1, valueFred1);
		collection.insert(timeKeyBob3, valueBob3);
		collection.insert(timeKeyJoe3, valueJoe3);
		
		List<BlueIndexInfo<? extends ValueKey, TestValue>> indexInfoList = new LinkedList<>();
		indexInfoList.add(new BlueIndexInfo<IntegerKey, TestValue>("test_index", IntegerKey.class, new TestRetrievalKeyExtractor()));
		indexInfoList.add(new BlueIndexInfo<IntegerKey, TestValue>("test_index_2", IntegerKey.class, new TestMultiRetrievalKeyExtractor()));
		collection.createIndices(indexInfoList);
		
		ReadWriteIndexOnDisk<IntegerKey, TestValue> indexOnDisk1 = (ReadWriteIndexOnDisk<IntegerKey, TestValue>) collection.getIndex("test_index", IntegerKey.class);
		assertEquals(justFred, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk1, integerKey1));
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk1, integerKey2));
		assertEquals(bobAndJoe, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk1, integerKey3));
		
		ReadWriteIndexOnDisk<IntegerKey, TestValue> indexOnDisk2 = (ReadWriteIndexOnDisk<IntegerKey, TestValue>) collection.getIndex("test_index_2", IntegerKey.class);
		assertEquals(justFred, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk2, integerKey1));
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk2, integerKey2));
		assertEquals(fredBobAndJoe, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk2, integerKey3));

		collection.delete(timeKeyFred1);
		collection.delete(timeKeyJoe3);

		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk1, integerKey1));
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk1, integerKey2));
		assertEquals(justBob, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk1, integerKey3));

		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk1, integerKey1));
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk1, integerKey2));
		assertEquals(justBob, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk1, integerKey3));
	}

	@Test
	public void test_createExisting_doesNotPopulateLegacyIndex() throws Exception {
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
		List<TestValue> justFred = Arrays.asList(valueFred1);

		collection.insert(timeKeyFred1, valueFred1);
		collection.insert(timeKeyBob3, valueBob3);
		collection.insert(timeKeyJoe3, valueJoe3);

		BlueIndex<IntegerKey, TestValue> index = collection.createIndex("test_index", IntegerKey.class, new TestRetrievalKeyExtractor());
		ReadWriteIndexOnDisk<IntegerKey, TestValue> indexOnDisk = (ReadWriteIndexOnDisk<IntegerKey, TestValue>) index;

		assertEquals(justFred, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey1));
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey2));
		assertEquals(bobAndJoe, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey3));
		
		//Delete initialized file and index data
		Files.delete(indexOnDisk.indexPath.resolve(ReadWriteIndexOnDisk.FILE_KEY_NEEDS_INITIALIZING));
		Files.list(indexOnDisk.indexPath)
			.filter(Files::isDirectory)
			.map(Path::toFile)
			.forEach(Blutils::recursiveDelete);

		//Confirm data is gone
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey1));
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey2));
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey3));
		
		index = collection.createIndex("test_index", IntegerKey.class, new TestRetrievalKeyExtractor());
		indexOnDisk = (ReadWriteIndexOnDisk<IntegerKey, TestValue>) index;
		
		//Confirm data wasn't recreated
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey1));
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey2));
		assertEquals(emptyList, getValuesByIndexForTargetIndexedInteger(collection, indexOnDisk, integerKey3));
	}

	@Test
	public void test_indexStartupCleansUpTempFiles() throws Exception {
		ReadWriteTimeCollectionOnDisk<TestValue> collection = getTimeCollection();

		Path indexPath = collection.getPath().resolve(".index").resolve("test_index");
		Files.createDirectories(indexPath);
		Path file1 = Files.createFile(indexPath.resolve("file1"));
		Path file2 = Files.createFile(indexPath.resolve("_tmp_file2"));
		Path file3 = Files.createFile(indexPath.resolve("file3"));
		Path file4 = Files.createFile(indexPath.resolve("_tmp_file4"));
		
		collection.createIndex("test_index", IntegerKey.class, new TestRetrievalKeyExtractor());
		assertTrue(Files.exists(indexPath));
		assertTrue(Files.exists(file1));
		assertFalse(Files.exists(file2));
		assertTrue(Files.exists(file3));
		assertFalse(Files.exists(file4));
	}

	@Test
	public void test_indexStartupCleanUpTempLockedFilesDoesntErrorOut() throws Exception {
		ReadWriteTimeCollectionOnDisk<TestValue> collection = getTimeCollection();

		Path indexPath = collection.getPath().resolve(".index").resolve("test_index");
		ReadWriteIndexOnDisk<IntegerKey, TestValue> index = (ReadWriteIndexOnDisk<IntegerKey, TestValue>) collection.createIndex("test_index", IntegerKey.class, new TestRetrievalKeyExtractor());
		Blutils.recursiveDelete(indexPath.toFile());
		assertFalse(Files.exists(indexPath));
		index.cleanupTempFiles(); //Shouldn't throw and exception if deleting the tmp files fails
	}
	
	@Test
	public void test_integerIndexCondition() throws BlueDbException {
		ReadWriteTimeCollectionOnDisk<TestValue> collection = getTimeCollection();
		BlueIndex<IntegerKey, TestValue> index = collection.createIndex("test_index", IntegerKey.class, new TestRetrievalKeyExtractor());
		
		TestValue valueFred1 = new TestValue("Fred", 1);
		TestValue valueBob3 = new TestValue("Bob", 3);
		TestValue valueJoe3 = new TestValue("Joe", 3);
		
		TimeKey timeKeyFred1 = createTimeKey(1, valueFred1);
		TimeKey timeKeyBob3 = createTimeKey(2, valueBob3);
		TimeKey timeKeyJoe3 = createTimeKey(3, valueJoe3);
		
		collection.insert(timeKeyFred1, valueFred1);
		collection.insert(timeKeyBob3, valueBob3);
		collection.insert(timeKeyJoe3, valueJoe3);
		
		
		List<TestValue> results = collection.query()
			.where(index.createIntegerIndexCondition().isGreaterThan(1))
			.getList();
		assertEquals(2, results.size());
		assertEquals(valueBob3, results.get(0));
		assertEquals(valueJoe3, results.get(1));
		
		try {
			index.createLongIndexCondition();
			fail("You shouldn't be able to create a long index condition from an integer index");
		} catch (UnsupportedIndexConditionTypeException e) { }
		
		try {
			index.createStringIndexCondition();
			fail("You shouldn't be able to create a String index condition from an integer index");
		} catch (UnsupportedIndexConditionTypeException e) { }
		
		try {
			index.createUUIDIndexCondition();
			fail("You shouldn't be able to create a UUID index condition from an integer index");
		} catch (UnsupportedIndexConditionTypeException e) { }
	}
	
	@Test
	public void test_longIndexCondition() throws BlueDbException {
		ReadWriteTimeCollectionOnDisk<TestValue> collection = getTimeCollection();
		BlueIndex<LongKey, TestValue> index = collection.createIndex("test_index", LongKey.class, new TestRetrievalLongKeyExtractor());
		
		TestValue valueFred1 = new TestValue("Fred", 1);
		TestValue valueBob3 = new TestValue("Bob", 3);
		TestValue valueJoe3 = new TestValue("Joe", 3);
		
		TimeKey timeKeyFred1 = createTimeKey(1, valueFred1);
		TimeKey timeKeyBob3 = createTimeKey(2, valueBob3);
		TimeKey timeKeyJoe3 = createTimeKey(3, valueJoe3);
		
		collection.insert(timeKeyFred1, valueFred1);
		collection.insert(timeKeyBob3, valueBob3);
		collection.insert(timeKeyJoe3, valueJoe3);
		
		
		List<TestValue> results = collection.query()
			.where(index.createLongIndexCondition().isGreaterThan(1))
			.getList();
		assertEquals(2, results.size());
		assertEquals(valueBob3, results.get(0));
		assertEquals(valueJoe3, results.get(1));
		
		try {
			index.createIntegerIndexCondition();
			fail("You shouldn't be able to create a integer index condition from an long index");
		} catch (UnsupportedIndexConditionTypeException e) { }
		
		try {
			index.createStringIndexCondition();
			fail("You shouldn't be able to create a String index condition from an long index");
		} catch (UnsupportedIndexConditionTypeException e) { }
		
		try {
			index.createUUIDIndexCondition();
			fail("You shouldn't be able to create a UUID index condition from an long index");
		} catch (UnsupportedIndexConditionTypeException e) { }
	}
	
	@Test
	public void test_stringIndexCondition() throws BlueDbException {
		ReadWriteTimeCollectionOnDisk<TestValue> collection = getTimeCollection();
		BlueIndex<StringKey, TestValue> index = collection.createIndex("test_index", StringKey.class, new TestRetrievalStringKeyExtractor());
		
		TestValue valueFred1 = new TestValue("Fred", 1);
		TestValue valueBob3 = new TestValue("Bob", 3);
		TestValue valueJoe3 = new TestValue("Joe", 3);
		
		TimeKey timeKeyFred1 = createTimeKey(1, valueFred1);
		TimeKey timeKeyBob3 = createTimeKey(2, valueBob3);
		TimeKey timeKeyJoe3 = createTimeKey(3, valueJoe3);
		
		collection.insert(timeKeyFred1, valueFred1);
		collection.insert(timeKeyBob3, valueBob3);
		collection.insert(timeKeyJoe3, valueJoe3);
		
		
		List<TestValue> results = collection.query()
			.where(index.createStringIndexCondition().isEqualTo("3"))
			.getList();
		assertEquals(2, results.size());
		assertEquals(valueBob3, results.get(0));
		assertEquals(valueJoe3, results.get(1));
		
		try {
			index.createIntegerIndexCondition();
			fail("You shouldn't be able to create an integer index condition from an string index");
		} catch (UnsupportedIndexConditionTypeException e) { }
		
		try {
			index.createLongIndexCondition();
			fail("You shouldn't be able to create a long index condition from a string index");
		} catch (UnsupportedIndexConditionTypeException e) { }
		
		try {
			index.createUUIDIndexCondition();
			fail("You shouldn't be able to create a UUID index condition from an string index");
		} catch (UnsupportedIndexConditionTypeException e) { }
	}
	
	@Test
	public void test_uuidIndexCondition() throws BlueDbException {
		ReadWriteTimeCollectionOnDisk<TestValue> collection = getTimeCollection();
		BlueIndex<UUIDKey, TestValue> index = collection.createIndex("test_index", UUIDKey.class, new TestRetrievalUUIDKeyExtractor());
		
		TestValue valueFred1 = new TestValue("Fred", 1);
		TestValue valueBob3 = new TestValue("Bob", 3);
		TestValue valueJoe3 = new TestValue("Joe", 3);
		
		TimeKey timeKeyFred1 = createTimeKey(1, valueFred1);
		TimeKey timeKeyBob3 = createTimeKey(2, valueBob3);
		TimeKey timeKeyJoe3 = createTimeKey(3, valueJoe3);
		
		collection.insert(timeKeyFred1, valueFred1);
		collection.insert(timeKeyBob3, valueBob3);
		collection.insert(timeKeyJoe3, valueJoe3);
		
		
		List<TestValue> results = collection.query()
			.where(index.createUUIDIndexCondition().isEqualTo(new UUID(3, 3)))
			.getList();
		assertEquals(2, results.size());
		assertEquals(valueBob3, results.get(0));
		assertEquals(valueJoe3, results.get(1));
		
		try {
			index.createIntegerIndexCondition();
			fail("You shouldn't be able to create an integer index condition from an UUID index");
		} catch (UnsupportedIndexConditionTypeException e) { }
		
		try {
			index.createLongIndexCondition();
			fail("You shouldn't be able to create a long index condition from a UUID index");
		} catch (UnsupportedIndexConditionTypeException e) { }
		
		try {
			index.createStringIndexCondition();
			fail("You shouldn't be able to create a String index condition from an UUID index");
		} catch (UnsupportedIndexConditionTypeException e) { }
	}
	
	@Test
	public void test_getIndexSegmentRanges() throws Exception {
		ReadWriteTimeCollectionOnDisk<TestValue> collection = getTimeCollection();
		ReadableIndexOnDisk<IntegerKey, TestValue> index = (ReadableIndexOnDisk<IntegerKey, TestValue>) collection.createIndex("test_index", IntegerKey.class, new TestRetrievalKeyExtractor());
		ReadWriteIndexOnDisk<IntegerKey, TestValue> indexOnDisk = (ReadWriteIndexOnDisk<IntegerKey, TestValue>) index;
		
		TestValue valueFred1 = new TestValue("Fred", 1);
		TimeKey timeKeyFred1 = createTimeKey(1, valueFred1);
		ValueKey indexKey = new IntegerKey(1);
		
		assertEquals(indexOnDisk.getSegmentManager().getSegmentRange(indexKey.getGroupingNumber()), index.getIndexSegmentRangeForIndexKey(indexKey));
		assertEquals(indexOnDisk.getSegmentManagerForIndexedCollection().getSegmentRange(timeKeyFred1.getGroupingNumber()), index.getCollectionSegmentRangeForValueKey(timeKeyFred1));
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
	
	@Test
	public void test_getSortedIndexChangesForValueUpdate() throws BlueDbException {
		ReadWriteIndexOnDisk<IntegerKey, TestValue> index = (ReadWriteIndexOnDisk<IntegerKey, TestValue>) getTimeCollection().createIndex("test_index", IntegerKey.class, new TestMultiKeyExtractor());
		
		TimeKey key = new TimeKey(1, 1);
		
		IndividualChange<BlueKey> add0 = createIndexChange(0, key, true);
		IndividualChange<BlueKey> add1 = createIndexChange(1, key, true);
		IndividualChange<BlueKey> add2 = createIndexChange(2, key, true);
		IndividualChange<BlueKey> add3 = createIndexChange(3, key, true);
		IndividualChange<BlueKey> add4 = createIndexChange(4, key, true);
		IndividualChange<BlueKey> add5 = createIndexChange(5, key, true);
		
		IndividualChange<BlueKey> remove1 = createIndexChange(1, key, false);
		IndividualChange<BlueKey> remove2 = createIndexChange(2, key, false);
		IndividualChange<BlueKey> remove3 = createIndexChange(3, key, false);
		IndividualChange<BlueKey> remove4 = createIndexChange(4, key, false);
		IndividualChange<BlueKey> remove5 = createIndexChange(5, key, false);
		
		TestValue oldValue = null;
		TestValue newValue = new TestValue("Fred", 5);
		
		assertEquals(Arrays.asList(add1, add3, add5), index.getSortedIndexChangesForValueChange(key, oldValue, newValue));
		
		oldValue = newValue;
		newValue = new TestValue("Fred", 4);
		assertEquals(Arrays.asList(add0, remove1, add2, remove3, add4, remove5), index.getSortedIndexChangesForValueChange(key, oldValue, newValue));
		
		oldValue = newValue;
		newValue = new TestValue("Fred", 4);
		assertEquals(Arrays.asList(), index.getSortedIndexChangesForValueChange(key, oldValue, newValue));
		
		oldValue = newValue;
		newValue = new TestValue("Fred", 2);
		assertEquals(Arrays.asList(remove4), index.getSortedIndexChangesForValueChange(key, oldValue, newValue));
		
		oldValue = newValue;
		newValue = new TestValue("Fred", 4);
		assertEquals(Arrays.asList(add4), index.getSortedIndexChangesForValueChange(key, oldValue, newValue));
		
		oldValue = newValue;
		newValue = new TestValue("Fred", 0);
		assertEquals(Arrays.asList(remove2, remove4), index.getSortedIndexChangesForValueChange(key, oldValue, newValue));
	}

	private IndividualChange<BlueKey> createIndexChange(int indexKey, BlueKey valueKey, boolean isAdd) {
		if(isAdd) {
			return new IndividualChange<BlueKey>(new IndexCompositeKey<BlueKey>(new IntegerKey(indexKey), valueKey), null, valueKey);
		} else {
			return new IndividualChange<BlueKey>(new IndexCompositeKey<BlueKey>(new IntegerKey(indexKey), valueKey), valueKey, null);
		}
	}

	private List<TestValue> getValuesByIndexForTargetIndexedInteger(ReadableBlueCollection<TestValue> collection, BlueIndex<IntegerKey, TestValue> index, IntegerKey targetIntegerKey) throws BlueDbException {
		return collection.query()
				.where(index.createIntegerIndexCondition().isEqualTo(targetIntegerKey.getId()))
				.getList();
	}
}
