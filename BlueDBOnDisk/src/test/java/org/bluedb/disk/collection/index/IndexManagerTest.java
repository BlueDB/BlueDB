package org.bluedb.disk.collection.index;

import java.util.Arrays;
import java.util.List;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.BlueTimeCollectionOnDisk;
import org.junit.Test;

public class IndexManagerTest extends BlueDbDiskTestBase {

	@Test
	public void test_getIndex() throws Exception {
		String indexName = "test_index";
		BlueTimeCollectionOnDisk<TestValue> collection = getTimeCollection();
		BlueIndex<IntegerKey, TestValue> index = collection.createIndex(indexName, IntegerKey.class, new TestRetrievalKeyExtractor());

		assertEquals(index, collection.getIndex(indexName, IntegerKey.class));
		
		try {
			collection.getIndex(indexName, LongKey.class);
			fail();
		} catch (BlueDbException e) {}
		//		BlueIndexOnDisk<IntegerKey, TestValue> indexOnDisk = (BlueIndexOnDisk<IntegerKey, TestValue>) index;

		BlueIndex<IntegerKey, TestValue> index2 = collection.createIndex(indexName, IntegerKey.class, new TestRetrievalKeyExtractor());
		assertTrue(index == index2);
	}

	@Test
	public void test_getIndexesFromDisk() throws Exception {
		String indexName = "test_index";
		BlueTimeCollectionOnDisk<TestValue> collection = getTimeCollection();
		collection.createIndex(indexName, IntegerKey.class, new TestRetrievalKeyExtractor());

		TestValue valueFred1 = new TestValue("Fred", 1);
		TestValue valueBob3 = new TestValue("Bob", 3);
		TestValue valueJoe3 = new TestValue("Joe", 3);
		TimeKey timeKeyFred1 = createTimeKey(1, valueFred1);
		TimeKey timeKeyBob3 = createTimeKey(2, valueBob3);
		TimeKey timeKeyJoe3 = createTimeKey(3, valueJoe3);
		IntegerKey integerKey1 = new IntegerKey(1);
		IntegerKey integerKey2 = new IntegerKey(2);
		IntegerKey integerKey3 = new IntegerKey(3);
		collection.insert(timeKeyFred1, valueFred1);
		collection.insert(timeKeyBob3, valueBob3);
		collection.insert(timeKeyJoe3, valueJoe3);

		List<BlueKey> emptyList = Arrays.asList();
		List<BlueKey> bobAndJoe = Arrays.asList(timeKeyBob3, timeKeyJoe3);
		List<BlueKey> justFred = Arrays.asList(timeKeyFred1);

		@SuppressWarnings({"rawtypes", "unchecked"})
		IndexManager<TestValue> restoredIndexManager = new IndexManager(collection, collection.getPath());
		BlueIndex<IntegerKey, TestValue> restoredIndex = restoredIndexManager.getIndex(indexName, IntegerKey.class);
		BlueIndexOnDisk<IntegerKey, TestValue> restoredIndexOnDisk = (BlueIndexOnDisk<IntegerKey, TestValue>) restoredIndex;

		assertEquals(justFred, restoredIndexOnDisk.getKeys(integerKey1));
		assertEquals(emptyList, restoredIndexOnDisk.getKeys(integerKey2));
		assertEquals(bobAndJoe, restoredIndexOnDisk.getKeys(integerKey3));

	}

	
}
