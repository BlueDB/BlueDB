package org.bluedb.disk.collection.index;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.BlueIndexInfo;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.ReadWriteTimeCollectionOnDisk;
import org.bluedb.disk.file.FileUtils;
import org.junit.Test;

public class IndexManagerTest extends BlueDbDiskTestBase {

	@Test
	public void test_getIndex() throws Exception {
		String indexName = "test_index";
		ReadWriteTimeCollectionOnDisk<TestValue> collection = getTimeCollection();
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
	public void test_getIndicesFromDisk() throws Exception {
		String index1Name = "test_index";
		String index2Name = "test_index_2";
		ReadWriteTimeCollectionOnDisk<TestValue> collection = getTimeCollection();
		
		List<BlueIndexInfo<? extends ValueKey, TestValue>> indexInfoList = new LinkedList<>();
		indexInfoList.add(new BlueIndexInfo<IntegerKey, TestValue>(index1Name, IntegerKey.class, new TestRetrievalKeyExtractor()));
		indexInfoList.add(new BlueIndexInfo<IntegerKey, TestValue>(index2Name, IntegerKey.class, new TestMultiRetrievalKeyExtractor()));
		collection.createIndices(indexInfoList);
		
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

		Set<BlueKey> emptyList = new HashSet<>(Arrays.asList());
		Set<BlueKey> bobAndJoe = new HashSet<>(Arrays.asList(timeKeyBob3, timeKeyJoe3));
		Set<BlueKey> fredBobAndJoe =  new HashSet<>(Arrays.asList(timeKeyFred1, timeKeyBob3, timeKeyJoe3));
		Set<BlueKey> justFred = new HashSet<>(Arrays.asList(timeKeyFred1));

		@SuppressWarnings({"rawtypes", "unchecked"})
		ReadWriteIndexManager<TestValue> restoredIndexManager = new ReadWriteIndexManager(collection, collection.getPath());
		
		ReadWriteIndexOnDisk<IntegerKey, TestValue> restoredIndexOnDisk1 = (ReadWriteIndexOnDisk<IntegerKey, TestValue>) restoredIndexManager.getIndex(index1Name, IntegerKey.class);
		assertEquals(justFred, restoredIndexOnDisk1.getKeys(integerKey1));
		assertEquals(emptyList, restoredIndexOnDisk1.getKeys(integerKey2));
		assertEquals(bobAndJoe, restoredIndexOnDisk1.getKeys(integerKey3));
		
		ReadWriteIndexOnDisk<IntegerKey, TestValue> restoredIndexOnDisk2 = (ReadWriteIndexOnDisk<IntegerKey, TestValue>) restoredIndexManager.getIndex(index2Name, IntegerKey.class);
		assertEquals(justFred, restoredIndexOnDisk2.getKeys(integerKey1));
		assertEquals(emptyList, restoredIndexOnDisk2.getKeys(integerKey2));
		assertEquals(fredBobAndJoe, restoredIndexOnDisk2.getKeys(integerKey3));
	}

	@Test
	public void test_createExistingIndices() throws Exception {
		String index1Name = "test_index";
		String index2Name = "test_index_2";
		ReadWriteTimeCollectionOnDisk<TestValue> collection = getTimeCollection();
		
		List<BlueIndexInfo<? extends ValueKey, TestValue>> indexInfoList = new LinkedList<>();
		indexInfoList.add(new BlueIndexInfo<IntegerKey, TestValue>(index1Name, IntegerKey.class, new TestRetrievalKeyExtractor()));
		indexInfoList.add(new BlueIndexInfo<IntegerKey, TestValue>(index2Name, IntegerKey.class, new TestMultiRetrievalKeyExtractor()));
		collection.createIndices(indexInfoList);
		
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

		Set<BlueKey> emptyList = new HashSet<>(Arrays.asList());
		Set<BlueKey> bobAndJoe = new HashSet<>(Arrays.asList(timeKeyBob3, timeKeyJoe3));
		Set<BlueKey> fredBobAndJoe =  new HashSet<>(Arrays.asList(timeKeyFred1, timeKeyBob3, timeKeyJoe3));
		Set<BlueKey> justFred = new HashSet<>(Arrays.asList(timeKeyFred1));
		
		ReadWriteIndexOnDisk<IntegerKey, TestValue> index1 = (ReadWriteIndexOnDisk<IntegerKey, TestValue>) collection.getIndex(index1Name, IntegerKey.class);
		ReadWriteIndexOnDisk<IntegerKey, TestValue> index2 = (ReadWriteIndexOnDisk<IntegerKey, TestValue>) collection.getIndex(index2Name, IntegerKey.class);
		
		//Delete index1 data and verify it worked
		deleteIndexDataAndMarkAsNotInitialized(index1);
		assertEquals(emptyList, index1.getKeys(integerKey1));
		assertEquals(emptyList, index1.getKeys(integerKey2));
		assertEquals(emptyList, index1.getKeys(integerKey3));
		
		//Calling createIndices should recreate the index data for index 1. Index 2 should fail since the key type is incorrect
		try {
			indexInfoList = new LinkedList<>();
			indexInfoList.add(new BlueIndexInfo<IntegerKey, TestValue>(index1Name, IntegerKey.class, new TestRetrievalKeyExtractor()));
			indexInfoList.add(new BlueIndexInfo<LongKey, TestValue>(index2Name, LongKey.class, new TestMultiRetrievalLongKeyExtractor()));
			collection.createIndices(indexInfoList);
		} catch(BlueDbException e) {
			//expected
		}
		
		assertEquals(justFred, index1.getKeys(integerKey1));
		assertEquals(emptyList, index1.getKeys(integerKey2));
		assertEquals(bobAndJoe, index1.getKeys(integerKey3));
		
		//Creating them again doesn't throw an exception or cause any problems.
		indexInfoList = new LinkedList<>();
		indexInfoList.add(new BlueIndexInfo<IntegerKey, TestValue>(index1Name, IntegerKey.class, new TestRetrievalKeyExtractor()));
		indexInfoList.add(new BlueIndexInfo<IntegerKey, TestValue>(index2Name, IntegerKey.class, new TestMultiRetrievalKeyExtractor()));
		collection.createIndices(indexInfoList);
		
		assertEquals(justFred, index1.getKeys(integerKey1));
		assertEquals(emptyList, index1.getKeys(integerKey2));
		assertEquals(bobAndJoe, index1.getKeys(integerKey3));
		
		assertEquals(justFred, index2.getKeys(integerKey1));
		assertEquals(emptyList, index2.getKeys(integerKey2));
		assertEquals(fredBobAndJoe, index2.getKeys(integerKey3));
	}

	private void deleteIndexDataAndMarkAsNotInitialized(ReadWriteIndexOnDisk<IntegerKey, TestValue> index) throws IOException, BlueDbException {
		FileUtils.getFolderContentsExcludingTempFilesAsStream(index.indexPath, "")
			.forEach(path -> {
				if(Files.isDirectory(path)) {
					Blutils.recursiveDelete(path.toFile());
				}
			});
		
		index.markAsNeedsInitialization();
	}

	
}
