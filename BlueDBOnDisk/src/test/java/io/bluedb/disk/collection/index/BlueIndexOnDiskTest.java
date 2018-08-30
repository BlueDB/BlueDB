package io.bluedb.disk.collection.index;

import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import io.bluedb.api.BlueIndex;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.IntegerKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.collection.BlueCollectionOnDisk;

public class BlueIndexOnDiskTest extends BlueDbDiskTestBase {

	@Test
	public void test_getKeys() throws Exception {
		BlueCollectionOnDisk<TestValue> collection = getTimeCollection();
		BlueIndex<IntegerKey, TestValue> index = collection.createIndex("test_index", IntegerKey.class, new TestRetrievalKeyExtractor());
		BlueIndexOnDisk<IntegerKey, TestValue> indexOnDisk = (BlueIndexOnDisk<IntegerKey, TestValue>) index;

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
	public void test_get() throws Exception {
		BlueCollectionOnDisk<TestValue> collection = getTimeCollection();
		BlueIndex<IntegerKey, TestValue> index = collection.createIndex("test_index", IntegerKey.class, new TestRetrievalKeyExtractor());
		BlueIndexOnDisk<IntegerKey, TestValue> indexOnDisk = (BlueIndexOnDisk<IntegerKey, TestValue>) index;

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
	}

	@Test
	public void test_rollup() throws Exception {
//		BlueCollectionOnDisk<TestValue> collection = getTimeCollection();
//		BlueIndex<IntegerKey, TestValue> index = collection.createIndex("test_index", IntegerKey.class, new TestRetrievalKeyExtractor());
//		BlueIndexOnDisk<IntegerKey, TestValue> indexOnDisk = (BlueIndexOnDisk<IntegerKey, TestValue>) index;
//
//		BlueKey key1At1 = createKey(1, 1);
//		BlueKey key3At3 = createKey(3, 3);
//		TestValue value1 = createValue("Anna", 1);
//		TestValue value3 = createValue("Chuck", 3);
//		List<TestValue> values;
//
//		values = collection.query().getList();
//		assertEquals(0, values.size());
//
//		collection.insert(key1At1, value1);
//		collection.insert(key3At3, value3);
//		values = collection.query().getList();
//		assertEquals(2, values.size());
//
//		Segment<TestValue> segment = index.getSegmentManager().getSegment(1);
//		File[] segmentDirectoryContents = segment.getPath().toFile().listFiles();
//		assertEquals(2, segmentDirectoryContents.length);
//
//		long segmentSize = getTimeCollection().getSegmentManager().getSegmentSize();
//		Range offByOneSegmentTimeRange = new Range(0, segmentSize);
//		Range entireFirstSegmentTimeRange = new Range(0, segmentSize -1);
//		try {
//			index.rollup(offByOneSegmentTimeRange);
//			fail();
//		} catch (BlueDbException e) {}
//		try {
//			index.rollup(entireFirstSegmentTimeRange);
//		} catch (BlueDbException e) {
//			fail();
//		}
//
//		values = collection.query().getList();
//		assertEquals(2, values.size());
//		segmentDirectoryContents = segment.getPath().toFile().listFiles();
//		assertEquals(1, segmentDirectoryContents.length);
	}
}
