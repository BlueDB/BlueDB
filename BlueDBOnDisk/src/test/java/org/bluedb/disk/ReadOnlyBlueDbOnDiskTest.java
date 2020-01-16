package org.bluedb.disk;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.ReadableBlueCollection;
import org.bluedb.api.ReadableBlueTimeCollection;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.TimeKey;
import org.junit.Test;

public class ReadOnlyBlueDbOnDiskTest extends BlueDbDiskTestBase {

	@Test
	public void test_getCollection() throws Exception {
		ReadOnlyBlueDbOnDisk readOnlyDb = (ReadOnlyBlueDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db.getPath()).buildReadOnly();
		ReadableBlueCollection<TestValue> collection = readOnlyDb.getTimeCollection(getTimeCollectionName(), TestValue.class);
		assertNotNull(collection);
		assertEquals(collection, readOnlyDb.getCollection(getTimeCollectionName(), TestValue.class));
		assertNull(db.getCollection("non-existing", TestValue.class));
	}

	@Test
	public void test_getCollection_wrong_type() throws Exception {
		ReadOnlyBlueDbOnDisk readOnlyDb = (ReadOnlyBlueDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db.getPath()).buildReadOnly();
		BlueCollection<TestValue> collection = db.getCollectionBuilder(getTimeCollectionName(), TimeKey.class, TestValue.class).build();
		TimeKey testValueKey = new TimeKey(1, 1);
		collection.insert(testValueKey, new TestValue("Bob"));
		try {
			readOnlyDb.getCollection(getTimeCollectionName(), TimeKey.class);
			readOnlyDb.getCollection(getTimeCollectionName(), String.class);
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_getTimeCollection() throws Exception {
		db.getTimeCollectionBuilder(getTimeCollectionName(), TimeKey.class, TestValue.class).build();
		ReadOnlyBlueDbOnDisk readOnlyDb = (ReadOnlyBlueDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db.getPath()).buildReadOnly();
		ReadableBlueCollection<TestValue> collection = readOnlyDb.getTimeCollection(getTimeCollectionName(), TestValue.class);
		assertNotNull(collection);
		assertEquals(collection, readOnlyDb.getTimeCollection(getTimeCollectionName(), TestValue.class));
		assertNull(db.getCollection("non-existing", TestValue.class));
	}

	@Test
	public void test_getTimeCollection_notTimeCollection() throws Exception {
		String collectionName = "valueCollectionName";
		BlueCollection<TestValue> valueCollection = db.getCollectionBuilder(collectionName, StringKey.class, TestValue.class).build();
		StringKey testValueKey = new StringKey("test");
		valueCollection.insert(testValueKey, new TestValue("Bob"));
		ReadOnlyBlueDbOnDisk readOnlyDb = (ReadOnlyBlueDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db.getPath()).buildReadOnly();
		try {
			readOnlyDb.getTimeCollection(collectionName, TestValue.class);
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_query_count() throws Exception {
		ReadOnlyBlueDbOnDisk readOnlyDb = (ReadOnlyBlueDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db.getPath()).buildReadOnly();
		String longCollectionName = getLongCollection().getPath().toFile().getName();
		ReadableBlueCollection<TestValue> readOnlyTimeCollection = readOnlyDb.getTimeCollection(getTimeCollectionName(), TestValue.class);
		ReadableBlueCollection<TestValue> readOnlyLongCollection = readOnlyDb.getCollection(longCollectionName, TestValue.class);
        assertEquals(0, readOnlyTimeCollection.query().count());
        insertAtTime(10, new TestValue("Joe", 0));
        assertEquals(1, readOnlyTimeCollection.query().count());
        assertEquals(0, readOnlyLongCollection.query().count());
        insertAtLong(10, new TestValue("Joe", 0));
        assertEquals(1, readOnlyLongCollection.query().count());
	}

	@Test
	public void test_query_where() throws Exception {
        ReadOnlyBlueDbOnDisk readOnlyDb = (ReadOnlyBlueDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db.getPath()).buildReadOnly();
        ReadableBlueTimeCollection<TestValue> collection = readOnlyDb.getTimeCollection(getTimeCollectionName(), TestValue.class);
        TestValue valueJoe = new TestValue("Joe");
        TestValue valueBob = new TestValue("Bob");
        insertAtTime(1, valueJoe);
        insertAtTime(2, valueBob);
        List<TestValue> storedValues;

        storedValues = collection.query().getList();
        assertEquals(2, storedValues.size());
        List<TestValue> joeOnly = collection.query().where((v) -> v.getName().equals("Joe")).getList();
        assertEquals(1, joeOnly.size());
        assertEquals(valueJoe, joeOnly.get(0));

        Iterator<TestValue> iter = collection.query().where((v) -> v.getName().equals("Bob")).getIterator();
        List<TestValue> onlyBob = new ArrayList<>();
        iter.forEachRemaining(onlyBob::add);
        assertEquals(1, onlyBob.size());
        assertFalse(onlyBob.contains(valueJoe));
        assertTrue(onlyBob.contains(valueBob));
	}

	@Test
	public void test_query_beforeTime_timeframe() throws Exception {
        ReadOnlyBlueDbOnDisk readOnlyDb = (ReadOnlyBlueDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db.getPath()).buildReadOnly();
        ReadableBlueTimeCollection<TestValue> collection = readOnlyDb.getTimeCollection(getTimeCollectionName(), TestValue.class);
        TestValue value1to2 = new TestValue("Joe");
        TestValue value2to3 = new TestValue("Bob");
        insertAtTimeFrame(1, 2, value1to2);
        insertAtTimeFrame(2, 3, value2to3);

        List<TestValue> before3 = collection.query().beforeTime(3).getList();
        List<TestValue> before2 = collection.query().beforeTime(2).getList();
        List<TestValue> before1 = collection.query().beforeTime(1).getList();
        assertEquals(2, before3.size());
        assertEquals(1, before2.size());
        assertEquals(0, before1.size());
        assertTrue(before3.contains(value2to3));
        assertTrue(before3.contains(value1to2));
        assertTrue(before2.contains(value1to2));
        assertFalse(before1.contains(value1to2));

        before3.clear();
        before2.clear();
        before1.clear();
        collection.query().beforeTime(3).getIterator().forEachRemaining(before3::add);
        collection.query().beforeTime(2).getIterator().forEachRemaining(before2::add);
        collection.query().beforeTime(1).getIterator().forEachRemaining(before1::add);
        assertEquals(2, before3.size());
        assertEquals(1, before2.size());
        assertEquals(0, before1.size());
        assertTrue(before3.contains(value2to3));
        assertTrue(before3.contains(value1to2));
        assertTrue(before2.contains(value1to2));
        assertFalse(before1.contains(value1to2));
	}

	@Test
	public void test_query_beforeTime() throws Exception {
        ReadOnlyBlueDbOnDisk readOnlyDb = (ReadOnlyBlueDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db.getPath()).buildReadOnly();
        ReadableBlueTimeCollection<TestValue> collection = readOnlyDb.getTimeCollection(getTimeCollectionName(), TestValue.class);
        TestValue valueAt1 = new TestValue("Joe");
        TestValue valueAt2 = new TestValue("Bob");
        insertAtTime(1, valueAt1);
        insertAtTime(2, valueAt2);

        List<TestValue> before3 = collection.query().beforeTime(3).getList();
        List<TestValue> before2 = collection.query().beforeTime(2).getList();
        List<TestValue> before1 = collection.query().beforeTime(1).getList();
        assertEquals(2, before3.size());
        assertEquals(1, before2.size());
        assertEquals(0, before1.size());
        assertTrue(before3.contains(valueAt2));
        assertTrue(before3.contains(valueAt1));
        assertTrue(before2.contains(valueAt1));
        assertFalse(before1.contains(valueAt1));

        before3.clear();
        before2.clear();
        before1.clear();
        collection.query().beforeTime(3).getIterator().forEachRemaining(before3::add);
        collection.query().beforeTime(2).getIterator().forEachRemaining(before2::add);
        collection.query().beforeTime(1).getIterator().forEachRemaining(before1::add);
        assertEquals(2, before3.size());
        assertEquals(1, before2.size());
        assertEquals(0, before1.size());
        assertTrue(before3.contains(valueAt2));
        assertTrue(before3.contains(valueAt1));
        assertTrue(before2.contains(valueAt1));
        assertFalse(before1.contains(valueAt1));
        }

	@Test
	public void test_query_beforeOrAtTime_timeframe() throws Exception {
        ReadOnlyBlueDbOnDisk readOnlyDb = (ReadOnlyBlueDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db.getPath()).buildReadOnly();
        ReadableBlueTimeCollection<TestValue> collection = readOnlyDb.getTimeCollection(getTimeCollectionName(), TestValue.class);
        TestValue value1to2 = new TestValue("Joe");
        TestValue value2to3 = new TestValue("Bob");
        insertAtTimeFrame(1, 2, value1to2);
        insertAtTimeFrame(2, 3, value2to3);

        List<TestValue> beforeOrAt3 = collection.query().beforeOrAtTime(3).getList();
        List<TestValue> beforeOrAt2 = collection.query().beforeOrAtTime(2).getList();
        List<TestValue> beforeOrAt1 = collection.query().beforeOrAtTime(1).getList();
        List<TestValue> beforeOrAt0 = collection.query().beforeOrAtTime(0).getList();
        assertEquals(2, beforeOrAt3.size());
        assertEquals(2, beforeOrAt2.size());
        assertEquals(1, beforeOrAt1.size());
        assertEquals(0, beforeOrAt0.size());
        assertTrue(beforeOrAt3.contains(value2to3));
        assertTrue(beforeOrAt3.contains(value1to2));
        assertTrue(beforeOrAt2.contains(value2to3));
        assertTrue(beforeOrAt2.contains(value1to2));
        assertTrue(beforeOrAt1.contains(value1to2));
        assertFalse(beforeOrAt0.contains(value1to2));

        beforeOrAt3.clear(); collection.query().beforeOrAtTime(3).getIterator().forEachRemaining(beforeOrAt3::add);
        beforeOrAt2.clear(); collection.query().beforeOrAtTime(2).getIterator().forEachRemaining(beforeOrAt2::add);
        beforeOrAt1.clear(); collection.query().beforeOrAtTime(1).getIterator().forEachRemaining(beforeOrAt1::add);
        beforeOrAt0.clear(); collection.query().beforeOrAtTime(0).getIterator().forEachRemaining(beforeOrAt0::add);
        assertEquals(2, beforeOrAt3.size());
        assertEquals(2, beforeOrAt2.size());
        assertEquals(1, beforeOrAt1.size());
        assertEquals(0, beforeOrAt0.size());
        assertTrue(beforeOrAt3.contains(value2to3));
        assertTrue(beforeOrAt3.contains(value1to2));
        assertTrue(beforeOrAt2.contains(value2to3));
        assertTrue(beforeOrAt2.contains(value1to2));
        assertTrue(beforeOrAt1.contains(value1to2));
        assertFalse(beforeOrAt0.contains(value1to2));
	}

	@Test
	public void test_query_beforeOrAtTime() throws Exception {
        ReadOnlyBlueDbOnDisk readOnlyDb = (ReadOnlyBlueDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db.getPath()).buildReadOnly();
        ReadableBlueTimeCollection<TestValue> collection = readOnlyDb.getTimeCollection(getTimeCollectionName(), TestValue.class);
        TestValue valueAt1 = new TestValue("Joe");
        TestValue valueAt2 = new TestValue("Bob");
        insertAtTime(1, valueAt1);
        insertAtTime(2, valueAt2);

        List<TestValue> beforeOrAt3 = collection.query().beforeOrAtTime(3).getList();
        List<TestValue> beforeOrAt2 = collection.query().beforeOrAtTime(2).getList();
        List<TestValue> beforeOrAt1 = collection.query().beforeOrAtTime(1).getList();
        assertEquals(2, beforeOrAt3.size());
        assertEquals(2, beforeOrAt2.size());
        assertEquals(1, beforeOrAt1.size());
        assertTrue(beforeOrAt3.contains(valueAt2));
        assertTrue(beforeOrAt3.contains(valueAt1));
        assertTrue(beforeOrAt2.contains(valueAt2));
        assertTrue(beforeOrAt2.contains(valueAt1));
        assertFalse(beforeOrAt1.contains(valueAt2));
        assertTrue(beforeOrAt1.contains(valueAt1));

        beforeOrAt3.clear(); collection.query().beforeOrAtTime(3).getIterator().forEachRemaining(beforeOrAt3::add);
        beforeOrAt2.clear(); collection.query().beforeOrAtTime(2).getIterator().forEachRemaining(beforeOrAt2::add);
        beforeOrAt1.clear(); collection.query().beforeOrAtTime(1).getIterator().forEachRemaining(beforeOrAt1::add);
        assertEquals(2, beforeOrAt3.size());
        assertEquals(2, beforeOrAt2.size());
        assertEquals(1, beforeOrAt1.size());
        assertTrue(beforeOrAt3.contains(valueAt2));
        assertTrue(beforeOrAt3.contains(valueAt1));
        assertTrue(beforeOrAt2.contains(valueAt2));
        assertTrue(beforeOrAt2.contains(valueAt1));
        assertFalse(beforeOrAt1.contains(valueAt2));
        assertTrue(beforeOrAt1.contains(valueAt1));
	}

	@Test
	public void test_query_afterTime_timeframe() throws Exception {
        ReadOnlyBlueDbOnDisk readOnlyDb = (ReadOnlyBlueDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db.getPath()).buildReadOnly();
        ReadableBlueTimeCollection<TestValue> collection = readOnlyDb.getTimeCollection(getTimeCollectionName(), TestValue.class);
        TestValue value1to2 = new TestValue("Joe");
        TestValue value2to3 = new TestValue("Bob");
        insertAtTimeFrame(1, 2, value1to2);
        insertAtTimeFrame(2, 3, value2to3);

        List<TestValue> after3 = collection.query().afterTime(3).getList();
        List<TestValue> after2 = collection.query().afterTime(2).getList();
        List<TestValue> after1 = collection.query().afterTime(1).getList();
        List<TestValue> after0 = collection.query().afterTime(0).getList();
        assertEquals(2, after0.size());
        assertEquals(2, after1.size());
        assertEquals(1, after2.size());
        assertEquals(0, after3.size());
        assertTrue(after0.contains(value2to3));
        assertTrue(after0.contains(value1to2));
        assertTrue(after1.contains(value2to3));
        assertTrue(after1.contains(value1to2));
        assertTrue(after2.contains(value2to3));
        assertFalse(after3.contains(value2to3));

        after3.clear(); collection.query().afterTime(3).getIterator().forEachRemaining(after3::add);
        after2.clear(); collection.query().afterTime(2).getIterator().forEachRemaining(after2::add);
        after1.clear(); collection.query().afterTime(1).getIterator().forEachRemaining(after1::add);
        after0.clear(); collection.query().afterTime(0).getIterator().forEachRemaining(after0::add);
        assertEquals(2, after0.size());
        assertEquals(2, after1.size());
        assertEquals(1, after2.size());
        assertEquals(0, after3.size());
        assertTrue(after0.contains(value2to3));
        assertTrue(after0.contains(value1to2));
        assertTrue(after1.contains(value2to3));
        assertTrue(after1.contains(value1to2));
        assertTrue(after2.contains(value2to3));
        assertFalse(after3.contains(value2to3));
	}

	@Test
	public void test_query_afterTime() throws Exception {
        ReadOnlyBlueDbOnDisk readOnlyDb = (ReadOnlyBlueDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db.getPath()).buildReadOnly();
        ReadableBlueTimeCollection<TestValue> collection = readOnlyDb.getTimeCollection(getTimeCollectionName(), TestValue.class);
        TestValue valueAt1 = new TestValue("Joe");
        TestValue valueAt2 = new TestValue("Bob");
        insertAtTime(1, valueAt1);
        insertAtTime(2, valueAt2);

        List<TestValue> after2 = collection.query().afterTime(2).getList();
        List<TestValue> after1 = collection.query().afterTime(1).getList();
        List<TestValue> after0 = collection.query().afterTime(0).getList();
        assertEquals(2, after0.size());
        assertEquals(1, after1.size());
        assertEquals(0, after2.size());
        assertTrue(after0.contains(valueAt2));
        assertTrue(after0.contains(valueAt1));
        assertTrue(after1.contains(valueAt2));
        assertFalse(after2.contains(valueAt2));

        after2.clear(); collection.query().afterTime(2).getIterator().forEachRemaining(after2::add);
        after1.clear(); collection.query().afterTime(1).getIterator().forEachRemaining(after1::add);
        after0.clear(); collection.query().afterTime(0).getIterator().forEachRemaining(after0::add);
        assertEquals(2, after0.size());
        assertEquals(1, after1.size());
        assertEquals(0, after2.size());
        assertTrue(after0.contains(valueAt2));
        assertTrue(after0.contains(valueAt1));
        assertTrue(after1.contains(valueAt2));
        assertFalse(after2.contains(valueAt2));
	}

	@Test
	public void test_query_afterOrAtTime_timeframe() throws Exception {
        ReadOnlyBlueDbOnDisk readOnlyDb = (ReadOnlyBlueDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db.getPath()).buildReadOnly();
        ReadableBlueTimeCollection<TestValue> collection = readOnlyDb.getTimeCollection(getTimeCollectionName(), TestValue.class);
        TestValue value1to2 = new TestValue("Joe");
        TestValue value2to3 = new TestValue("Bob");
        insertAtTimeFrame(1, 2, value1to2);
        insertAtTimeFrame(2, 3, value2to3);

        List<TestValue> afterOrAt4 = collection.query().afterOrAtTime(4).getList();
        List<TestValue> afterOrAt3 = collection.query().afterOrAtTime(3).getList();
        List<TestValue> afterOrAt2 = collection.query().afterOrAtTime(2).getList();
        List<TestValue> afterOrAt1 = collection.query().afterOrAtTime(1).getList();
        List<TestValue> afterOrAt0 = collection.query().afterOrAtTime(0).getList();
        assertEquals(2, afterOrAt0.size());
        assertEquals(2, afterOrAt1.size());
        assertEquals(2, afterOrAt2.size());
        assertEquals(1, afterOrAt3.size());
        assertEquals(0, afterOrAt4.size());
        assertTrue(afterOrAt0.contains(value2to3));
        assertTrue(afterOrAt0.contains(value1to2));
        assertTrue(afterOrAt1.contains(value2to3));
        assertTrue(afterOrAt1.contains(value1to2));
        assertTrue(afterOrAt2.contains(value2to3));
        assertTrue(afterOrAt2.contains(value1to2));
        assertTrue(afterOrAt3.contains(value2to3));
        assertFalse(afterOrAt4.contains(value2to3));

        afterOrAt4.clear(); collection.query().afterOrAtTime(4).getIterator().forEachRemaining(afterOrAt4::add);
        afterOrAt3.clear(); collection.query().afterOrAtTime(3).getIterator().forEachRemaining(afterOrAt3::add);
        afterOrAt2.clear(); collection.query().afterOrAtTime(2).getIterator().forEachRemaining(afterOrAt2::add);
        afterOrAt1.clear(); collection.query().afterOrAtTime(1).getIterator().forEachRemaining(afterOrAt1::add);
        afterOrAt0.clear(); collection.query().afterOrAtTime(0).getIterator().forEachRemaining(afterOrAt0::add);
        assertEquals(2, afterOrAt0.size());
        assertEquals(2, afterOrAt1.size());
        assertEquals(2, afterOrAt2.size());
        assertEquals(1, afterOrAt3.size());
        assertEquals(0, afterOrAt4.size());
        assertTrue(afterOrAt0.contains(value2to3));
        assertTrue(afterOrAt0.contains(value1to2));
        assertTrue(afterOrAt1.contains(value2to3));
        assertTrue(afterOrAt1.contains(value1to2));
        assertTrue(afterOrAt2.contains(value2to3));
        assertTrue(afterOrAt2.contains(value1to2));
        assertTrue(afterOrAt3.contains(value2to3));
        assertFalse(afterOrAt4.contains(value2to3));
    }

	@Test
	public void test_query_afterOrAtTime() throws Exception {
        ReadOnlyBlueDbOnDisk readOnlyDb = (ReadOnlyBlueDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db.getPath()).buildReadOnly();
        ReadableBlueTimeCollection<TestValue> collection = readOnlyDb.getTimeCollection(getTimeCollectionName(), TestValue.class);
        TestValue valueAt1 = new TestValue("Joe");
        TestValue valueAt2 = new TestValue("Bob");
        insertAtTime(1, valueAt1);
        insertAtTime(2, valueAt2);

        List<TestValue> afterOrAt2 = collection.query().afterOrAtTime(2).getList();
        List<TestValue> afterOrAt1 = collection.query().afterOrAtTime(1).getList();
        List<TestValue> afterOrAt0 = collection.query().afterOrAtTime(0).getList();
        assertEquals(2, afterOrAt0.size());
        assertEquals(2, afterOrAt1.size());
        assertEquals(1, afterOrAt2.size());
        assertTrue(afterOrAt0.contains(valueAt2));
        assertTrue(afterOrAt0.contains(valueAt1));
        assertTrue(afterOrAt1.contains(valueAt2));
        assertTrue(afterOrAt1.contains(valueAt1));
        assertTrue(afterOrAt2.contains(valueAt2));
        assertFalse(afterOrAt2.contains(valueAt1));

        afterOrAt2.clear(); collection.query().afterOrAtTime(2).getIterator().forEachRemaining(afterOrAt2::add);
        afterOrAt1.clear(); collection.query().afterOrAtTime(1).getIterator().forEachRemaining(afterOrAt1::add);
        afterOrAt0.clear(); collection.query().afterOrAtTime(0).getIterator().forEachRemaining(afterOrAt0::add);
        assertEquals(2, afterOrAt0.size());
        assertEquals(2, afterOrAt1.size());
        assertEquals(1, afterOrAt2.size());
        assertTrue(afterOrAt0.contains(valueAt2));
        assertTrue(afterOrAt0.contains(valueAt1));
        assertTrue(afterOrAt1.contains(valueAt2));
        assertTrue(afterOrAt1.contains(valueAt1));
        assertTrue(afterOrAt2.contains(valueAt2));
        assertFalse(afterOrAt2.contains(valueAt1));
	}

	@Test
	public void test_query_Between() throws Exception {
        ReadOnlyBlueDbOnDisk readOnlyDb = (ReadOnlyBlueDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db.getPath()).buildReadOnly();
        ReadableBlueTimeCollection<TestValue> collection = readOnlyDb.getTimeCollection(getTimeCollectionName(), TestValue.class);
        TestValue valueAt2 = new TestValue("Joe");
        TestValue valueAt3 = new TestValue("Bob");
        insertAtTime(2, valueAt2);
        insertAtTime(3, valueAt3);

        // various queries outside the range
        List<TestValue> after0before1 = collection.query().afterTime(0).beforeTime(1).getList();
        List<TestValue> after0beforeOrAt1 = collection.query().afterTime(0).beforeOrAtTime(1).getList();
        List<TestValue> afterOrAt0before1 = collection.query().afterOrAtTime(0).beforeTime(1).getList();
        List<TestValue> afterOrAt0beforeOrAt1 = collection.query().afterOrAtTime(0).beforeOrAtTime(1).getList();
        assertEquals(0, after0before1.size());
        assertEquals(0, after0beforeOrAt1.size());
        assertEquals(0, afterOrAt0before1.size());
        assertEquals(0, afterOrAt0beforeOrAt1.size());

        // various queries inside the range
        List<TestValue> after2before3 = collection.query().afterTime(2).beforeTime(3).getList();
        List<TestValue> after2beforeOrAt3 = collection.query().afterTime(2).beforeOrAtTime(3).getList();
        List<TestValue> afterOrAt2before3 = collection.query().afterOrAtTime(2).beforeTime(3).getList();
        List<TestValue> afterOrAt2beforeOrAt3 = collection.query().afterOrAtTime(2).beforeOrAtTime(3).getList();
        assertEquals(0, after2before3.size());
        assertEquals(1, after2beforeOrAt3.size());
        assertEquals(1, afterOrAt2before3.size());
        assertEquals(2, afterOrAt2beforeOrAt3.size());
        assertFalse(after2beforeOrAt3.contains(valueAt2));
        assertTrue(after2beforeOrAt3.contains(valueAt3));
        assertTrue(afterOrAt2before3.contains(valueAt2));
        assertFalse(afterOrAt2before3.contains(valueAt3));
        assertTrue(afterOrAt2beforeOrAt3.contains(valueAt2));
        assertTrue(afterOrAt2beforeOrAt3.contains(valueAt3));
	}

	@Test
	public void test_getList() throws Exception {
        ReadOnlyBlueDbOnDisk readOnlyDb = (ReadOnlyBlueDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db.getPath()).buildReadOnly();
        ReadableBlueTimeCollection<TestValue> collection = readOnlyDb.getTimeCollection(getTimeCollectionName(), TestValue.class);
        TestValue valueJoe = new TestValue("Joe");
        TestValue valueBob = new TestValue("Bob");
        insertAtTime(1, valueJoe);
        insertAtTime(2, valueBob);
        List<TestValue> storedValues;

        storedValues = collection.query().getList();
        assertEquals(2, storedValues.size());
        List<TestValue> joeOnly = collection.query().where((v) -> v.getName().equals("Joe")).getList();
        assertEquals(1, joeOnly.size());
        assertEquals(valueJoe, joeOnly.get(0));
	}
	
	@Test
	public void test_getList_byStartTime() throws Exception {
        ReadOnlyBlueDbOnDisk readOnlyDb = (ReadOnlyBlueDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db.getPath()).buildReadOnly();
        ReadableBlueTimeCollection<TestValue> collection = readOnlyDb.getTimeCollection(getTimeCollectionName(), TestValue.class);
        TestValue valueJoe = new TestValue("Joe");
        TestValue valueBob = new TestValue("Bob");
        insertAtTimeFrame(1, 2, valueJoe);
        insertAtTimeFrame(2, 3, valueBob);
        List<TestValue> both = Arrays.asList(valueJoe, valueBob);
//        List<TestValue> justJoe = Arrays.asList(valueJoe);
        List<TestValue> justBob = Arrays.asList(valueBob);
        List<TestValue> neither = Arrays.asList();

        List<TestValue> values0to0 = collection.query().byStartTime().afterOrAtTime(0).beforeOrAtTime(0).getList();
        List<TestValue> values1to2 = collection.query().byStartTime().afterOrAtTime(1).beforeOrAtTime(2).getList();
        List<TestValue> values2to3 = collection.query().byStartTime().afterOrAtTime(2).beforeOrAtTime(3).getList();
        List<TestValue> values3to4 = collection.query().byStartTime().afterOrAtTime(3).beforeOrAtTime(4).getList();

        assertEquals(neither, values0to0);
        assertEquals(both, values1to2);
        assertEquals(justBob, values2to3);
        assertEquals(neither, values3to4);
	}	

	@Test
	public void test_shutdown() {
        ReadOnlyBlueDbOnDisk readOnlyDb = (ReadOnlyBlueDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db.getPath()).buildReadOnly();
        readOnlyDb.shutdown();
	}

	@Test
	public void test_shutdownNow() {
        ReadOnlyBlueDbOnDisk readOnlyDb = (ReadOnlyBlueDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db.getPath()).buildReadOnly();
        readOnlyDb.shutdownNow();
	}

	@Test
	public void test_awaitTermination() throws BlueDbException {
        ReadOnlyBlueDbOnDisk readOnlyDb = (ReadOnlyBlueDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db.getPath()).buildReadOnly();
        readOnlyDb.awaitTermination(1, TimeUnit.MICROSECONDS);
	}
	
}
