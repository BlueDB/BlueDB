package org.bluedb.disk;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.bluedb.TestUtils;
import org.bluedb.api.BlueCollection;
import org.bluedb.api.BlueQuery;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.HashGroupedKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.collection.BlueCollectionOnDisk;
import org.bluedb.disk.collection.BlueTimeCollectionOnDisk;
import org.bluedb.disk.collection.CollectionMetaData;
import org.bluedb.disk.collection.ReadOnlyBlueCollectionOnDisk;
import org.bluedb.disk.file.FileManager;
import org.bluedb.tasks.AsynchronousTestTask;
import org.bluedb.tasks.TestTask;
import org.bluedb.zip.ZipUtils;
import org.junit.Test;

public class BlueDbOnDiskTest extends BlueDbDiskTestBase {

	@Test
    public void test_getUntypedCollectionForBackup() throws Exception {
        String timeCollectionName = getTimeCollectionName();
		BlueCollectionOnDisk<String> newCollection = (BlueCollectionOnDisk<String>) db.getCollectionBuilder("new_collection", TimeKey.class, String.class).build();
        assertNotNull(db.getUntypedCollectionForBackup(timeCollectionName));
        assertNotNull(db.getUntypedCollectionForBackup("new_collection"));

        // now let's break the new collection
        Path newCollectionPath = newCollection.getPath();
        Path serializedClassesPath = Paths.get(newCollectionPath.toString(), ".meta", "serialized_classes");
        getFileManager().saveObject(serializedClassesPath, "some_nonsense");  // serialize a string where there should be a list


        BlueDbOnDisk reopenedDatbase = (BlueDbOnDisk) new BlueDbOnDiskBuilder().withPath(getPath()).build();
        assertNotNull(reopenedDatbase.getUntypedCollectionForBackup(timeCollectionName));  // this one isn't broken
		try {
			reopenedDatbase.getUntypedCollectionForBackup("new_collection"); // we broke it above
			fail();
		} catch (Throwable e) {
		}
	}

	@Test
	public void test_getCollection() throws Exception {
		db.collectionBuilder(getTimeCollectionName(), TimeKey.class, TestValue.class).build();

		BlueCollection<TestValue> collection = db.getTimeCollection(getTimeCollectionName(), TestValue.class);
		assertNotNull(collection);
		assertEquals(collection, db.getCollection(getTimeCollectionName(), TestValue.class));
		assertNull(db.getCollection("non-existing", TestValue.class));
	}

	@Test
	public void test_getCollection_wrong_type() throws Exception {
		BlueCollection<TestValue> valueCollection = db.collectionBuilder(getTimeCollectionName(), TimeKey.class, TestValue.class).build();
		TimeKey testValueKey = new TimeKey(1, 1);
		valueCollection.insert(testValueKey, new TestValue("Bob"));
		try {
			db.getCollection(getTimeCollectionName(), String.class);
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_getTimeCollection() throws Exception {
		db.getTimeCollectionBuilder(getTimeCollectionName(), TimeKey.class, TestValue.class).build();

		BlueCollection<TestValue> collection = db.getTimeCollection(getTimeCollectionName(), TestValue.class);
		assertNotNull(collection);
		assertEquals(collection, db.getTimeCollection(getTimeCollectionName(), TestValue.class));
		assertNull(db.getCollection("non-existing", TestValue.class));
	}

	@Test
	public void test_getTimeCollection_notTimeCollection() throws Exception {
		String collectionName = "valueCollectionName";
		BlueCollection<TestValue> valueCollection = db.getCollectionBuilder(collectionName, StringKey.class, TestValue.class).build();
		StringKey testValueKey = new StringKey("test");
		valueCollection.insert(testValueKey, new TestValue("Bob"));
		try {
			db.getTimeCollection(collectionName, TestValue.class);
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_initializeCollection_existing_correct_type() throws Exception {
		db.collectionBuilder(getTimeCollectionName(), TimeKey.class, TestValue.class).build();
		assertNotNull(db.collectionBuilder(getTimeCollectionName(), TimeKey.class, TestValue.class).build());  // make sure it works the second time as well
	}

	@SuppressWarnings({ "unchecked", "deprecation" })
	@Test
	public void test_initializeCollection_old() {
		insertAtTime(10, new TestValue("Bob"));
		try {
			db.initializeCollection(getTimeCollectionName(), TimeKey.class, TestValue.class, TestValue2.class);
		} catch(BlueDbException e) {
			fail();
		}
	}

	@Test
	public void test_initializeCollection_invalid_type() {
		insertAtTime(10, new TestValue("Bob"));
		try {
			db.initializeCollection(getTimeCollectionName(), TimeKey.class, TestValue2.class, Arrays.asList());
			fail();
		} catch(BlueDbException e) {
		}
	}

	@Test
	public void test_initializeCollection_invalid_key_type() {
		try {
			db.initializeCollection(getTimeCollectionName(), HashGroupedKey.class, TestValue.class, Arrays.asList());
			fail();
		} catch(BlueDbException e) {
		}
	}
	
	@Test
	public void test_query_count() throws Exception {
        assertEquals(0, getTimeCollection().query().count());
        BlueKey key = insertAtTime(10, new TestValue("Joe", 0));
        assertEquals(1, getTimeCollection().query().count());
        getTimeCollection().delete(key);
        assertEquals(0, getTimeCollection().query().count());
	}

	@Test
	public void test_query_where() throws Exception {
        TestValue valueJoe = new TestValue("Joe");
        TestValue valueBob = new TestValue("Bob");
        insertAtTime(1, valueJoe);
        insertAtTime(2, valueBob);
        List<TestValue> storedValues;

        storedValues = getTimeCollection().query().getList();
        assertEquals(2, storedValues.size());
        List<TestValue> joeOnly = getTimeCollection().query().where((v) -> v.getName().equals("Joe")).getList();
        assertEquals(1, joeOnly.size());
        assertEquals(valueJoe, joeOnly.get(0));

        Iterator<TestValue> iter = getTimeCollection().query().where((v) -> v.getName().equals("Bob")).getIterator();
        List<TestValue> onlyBob = new ArrayList<>();
        iter.forEachRemaining(onlyBob::add);
        assertEquals(1, onlyBob.size());
        assertFalse(onlyBob.contains(valueJoe));
        assertTrue(onlyBob.contains(valueBob));
	}

	@Test
	public void test_query_beforeTime_timeframe() throws Exception {
        TestValue value1to2 = new TestValue("Joe");
        TestValue value2to3 = new TestValue("Bob");
        insertAtTimeFrame(1, 2, value1to2);
        insertAtTimeFrame(2, 3, value2to3);

        List<TestValue> before3 = getTimeCollection().query().beforeTime(3).getList();
        List<TestValue> before2 = getTimeCollection().query().beforeTime(2).getList();
        List<TestValue> before1 = getTimeCollection().query().beforeTime(1).getList();
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
        getTimeCollection().query().beforeTime(3).getIterator().forEachRemaining(before3::add);
        getTimeCollection().query().beforeTime(2).getIterator().forEachRemaining(before2::add);
        getTimeCollection().query().beforeTime(1).getIterator().forEachRemaining(before1::add);
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
        TestValue valueAt1 = new TestValue("Joe");
        TestValue valueAt2 = new TestValue("Bob");
        insertAtTime(1, valueAt1);
        insertAtTime(2, valueAt2);

        List<TestValue> before3 = getTimeCollection().query().beforeTime(3).getList();
        List<TestValue> before2 = getTimeCollection().query().beforeTime(2).getList();
        List<TestValue> before1 = getTimeCollection().query().beforeTime(1).getList();
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
        getTimeCollection().query().beforeTime(3).getIterator().forEachRemaining(before3::add);
        getTimeCollection().query().beforeTime(2).getIterator().forEachRemaining(before2::add);
        getTimeCollection().query().beforeTime(1).getIterator().forEachRemaining(before1::add);
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
        TestValue value1to2 = new TestValue("Joe");
        TestValue value2to3 = new TestValue("Bob");
        insertAtTimeFrame(1, 2, value1to2);
        insertAtTimeFrame(2, 3, value2to3);

        List<TestValue> beforeOrAt3 = getTimeCollection().query().beforeOrAtTime(3).getList();
        List<TestValue> beforeOrAt2 = getTimeCollection().query().beforeOrAtTime(2).getList();
        List<TestValue> beforeOrAt1 = getTimeCollection().query().beforeOrAtTime(1).getList();
        List<TestValue> beforeOrAt0 = getTimeCollection().query().beforeOrAtTime(0).getList();
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

        beforeOrAt3.clear(); getTimeCollection().query().beforeOrAtTime(3).getIterator().forEachRemaining(beforeOrAt3::add);
        beforeOrAt2.clear(); getTimeCollection().query().beforeOrAtTime(2).getIterator().forEachRemaining(beforeOrAt2::add);
        beforeOrAt1.clear(); getTimeCollection().query().beforeOrAtTime(1).getIterator().forEachRemaining(beforeOrAt1::add);
        beforeOrAt0.clear(); getTimeCollection().query().beforeOrAtTime(0).getIterator().forEachRemaining(beforeOrAt0::add);
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
        TestValue valueAt1 = new TestValue("Joe");
        TestValue valueAt2 = new TestValue("Bob");
        insertAtTime(1, valueAt1);
        insertAtTime(2, valueAt2);

        List<TestValue> beforeOrAt3 = getTimeCollection().query().beforeOrAtTime(3).getList();
        List<TestValue> beforeOrAt2 = getTimeCollection().query().beforeOrAtTime(2).getList();
        List<TestValue> beforeOrAt1 = getTimeCollection().query().beforeOrAtTime(1).getList();
        assertEquals(2, beforeOrAt3.size());
        assertEquals(2, beforeOrAt2.size());
        assertEquals(1, beforeOrAt1.size());
        assertTrue(beforeOrAt3.contains(valueAt2));
        assertTrue(beforeOrAt3.contains(valueAt1));
        assertTrue(beforeOrAt2.contains(valueAt2));
        assertTrue(beforeOrAt2.contains(valueAt1));
        assertFalse(beforeOrAt1.contains(valueAt2));
        assertTrue(beforeOrAt1.contains(valueAt1));

        beforeOrAt3.clear(); getTimeCollection().query().beforeOrAtTime(3).getIterator().forEachRemaining(beforeOrAt3::add);
        beforeOrAt2.clear(); getTimeCollection().query().beforeOrAtTime(2).getIterator().forEachRemaining(beforeOrAt2::add);
        beforeOrAt1.clear(); getTimeCollection().query().beforeOrAtTime(1).getIterator().forEachRemaining(beforeOrAt1::add);
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
        TestValue value1to2 = new TestValue("Joe");
        TestValue value2to3 = new TestValue("Bob");
        insertAtTimeFrame(1, 2, value1to2);
        insertAtTimeFrame(2, 3, value2to3);

        List<TestValue> after3 = getTimeCollection().query().afterTime(3).getList();
        List<TestValue> after2 = getTimeCollection().query().afterTime(2).getList();
        List<TestValue> after1 = getTimeCollection().query().afterTime(1).getList();
        List<TestValue> after0 = getTimeCollection().query().afterTime(0).getList();
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

        after3.clear(); getTimeCollection().query().afterTime(3).getIterator().forEachRemaining(after3::add);
        after2.clear(); getTimeCollection().query().afterTime(2).getIterator().forEachRemaining(after2::add);
        after1.clear(); getTimeCollection().query().afterTime(1).getIterator().forEachRemaining(after1::add);
        after0.clear(); getTimeCollection().query().afterTime(0).getIterator().forEachRemaining(after0::add);
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
        TestValue valueAt1 = new TestValue("Joe");
        TestValue valueAt2 = new TestValue("Bob");
        insertAtTime(1, valueAt1);
        insertAtTime(2, valueAt2);

        List<TestValue> after2 = getTimeCollection().query().afterTime(2).getList();
        List<TestValue> after1 = getTimeCollection().query().afterTime(1).getList();
        List<TestValue> after0 = getTimeCollection().query().afterTime(0).getList();
        assertEquals(2, after0.size());
        assertEquals(1, after1.size());
        assertEquals(0, after2.size());
        assertTrue(after0.contains(valueAt2));
        assertTrue(after0.contains(valueAt1));
        assertTrue(after1.contains(valueAt2));
        assertFalse(after2.contains(valueAt2));

        after2.clear(); getTimeCollection().query().afterTime(2).getIterator().forEachRemaining(after2::add);
        after1.clear(); getTimeCollection().query().afterTime(1).getIterator().forEachRemaining(after1::add);
        after0.clear(); getTimeCollection().query().afterTime(0).getIterator().forEachRemaining(after0::add);
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
        TestValue value1to2 = new TestValue("Joe");
        TestValue value2to3 = new TestValue("Bob");
        insertAtTimeFrame(1, 2, value1to2);
        insertAtTimeFrame(2, 3, value2to3);

        List<TestValue> afterOrAt4 = getTimeCollection().query().afterOrAtTime(4).getList();
        List<TestValue> afterOrAt3 = getTimeCollection().query().afterOrAtTime(3).getList();
        List<TestValue> afterOrAt2 = getTimeCollection().query().afterOrAtTime(2).getList();
        List<TestValue> afterOrAt1 = getTimeCollection().query().afterOrAtTime(1).getList();
        List<TestValue> afterOrAt0 = getTimeCollection().query().afterOrAtTime(0).getList();
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

        afterOrAt4.clear(); getTimeCollection().query().afterOrAtTime(4).getIterator().forEachRemaining(afterOrAt4::add);
        afterOrAt3.clear(); getTimeCollection().query().afterOrAtTime(3).getIterator().forEachRemaining(afterOrAt3::add);
        afterOrAt2.clear(); getTimeCollection().query().afterOrAtTime(2).getIterator().forEachRemaining(afterOrAt2::add);
        afterOrAt1.clear(); getTimeCollection().query().afterOrAtTime(1).getIterator().forEachRemaining(afterOrAt1::add);
        afterOrAt0.clear(); getTimeCollection().query().afterOrAtTime(0).getIterator().forEachRemaining(afterOrAt0::add);
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
        TestValue valueAt1 = new TestValue("Joe");
        TestValue valueAt2 = new TestValue("Bob");
        insertAtTime(1, valueAt1);
        insertAtTime(2, valueAt2);

        List<TestValue> afterOrAt2 = getTimeCollection().query().afterOrAtTime(2).getList();
        List<TestValue> afterOrAt1 = getTimeCollection().query().afterOrAtTime(1).getList();
        List<TestValue> afterOrAt0 = getTimeCollection().query().afterOrAtTime(0).getList();
        assertEquals(2, afterOrAt0.size());
        assertEquals(2, afterOrAt1.size());
        assertEquals(1, afterOrAt2.size());
        assertTrue(afterOrAt0.contains(valueAt2));
        assertTrue(afterOrAt0.contains(valueAt1));
        assertTrue(afterOrAt1.contains(valueAt2));
        assertTrue(afterOrAt1.contains(valueAt1));
        assertTrue(afterOrAt2.contains(valueAt2));
        assertFalse(afterOrAt2.contains(valueAt1));

        afterOrAt2.clear(); getTimeCollection().query().afterOrAtTime(2).getIterator().forEachRemaining(afterOrAt2::add);
        afterOrAt1.clear(); getTimeCollection().query().afterOrAtTime(1).getIterator().forEachRemaining(afterOrAt1::add);
        afterOrAt0.clear(); getTimeCollection().query().afterOrAtTime(0).getIterator().forEachRemaining(afterOrAt0::add);
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
        TestValue valueAt2 = new TestValue("Joe");
        TestValue valueAt3 = new TestValue("Bob");
        insertAtTime(2, valueAt2);
        insertAtTime(3, valueAt3);

        // various queries outside the range
        List<TestValue> after0before1 = getTimeCollection().query().afterTime(0).beforeTime(1).getList();
        List<TestValue> after0beforeOrAt1 = getTimeCollection().query().afterTime(0).beforeOrAtTime(1).getList();
        List<TestValue> afterOrAt0before1 = getTimeCollection().query().afterOrAtTime(0).beforeTime(1).getList();
        List<TestValue> afterOrAt0beforeOrAt1 = getTimeCollection().query().afterOrAtTime(0).beforeOrAtTime(1).getList();
        assertEquals(0, after0before1.size());
        assertEquals(0, after0beforeOrAt1.size());
        assertEquals(0, afterOrAt0before1.size());
        assertEquals(0, afterOrAt0beforeOrAt1.size());

        // various queries inside the range
        List<TestValue> after2before3 = getTimeCollection().query().afterTime(2).beforeTime(3).getList();
        List<TestValue> after2beforeOrAt3 = getTimeCollection().query().afterTime(2).beforeOrAtTime(3).getList();
        List<TestValue> afterOrAt2before3 = getTimeCollection().query().afterOrAtTime(2).beforeTime(3).getList();
        List<TestValue> afterOrAt2beforeOrAt3 = getTimeCollection().query().afterOrAtTime(2).beforeOrAtTime(3).getList();
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
        TestValue valueJoe = new TestValue("Joe");
        TestValue valueBob = new TestValue("Bob");
        insertAtTime(1, valueJoe);
        insertAtTime(2, valueBob);
        List<TestValue> storedValues;

        storedValues = getTimeCollection().query().getList();
        assertEquals(2, storedValues.size());
        List<TestValue> joeOnly = getTimeCollection().query().where((v) -> v.getName().equals("Joe")).getList();
        assertEquals(1, joeOnly.size());
        assertEquals(valueJoe, joeOnly.get(0));
	}
	
	@Test
	public void test_getList_byStartTime() throws Exception {
        TestValue valueJoe = new TestValue("Joe");
        TestValue valueBob = new TestValue("Bob");
        insertAtTimeFrame(1, 2, valueJoe);
        insertAtTimeFrame(2, 3, valueBob);
        List<TestValue> both = Arrays.asList(valueJoe, valueBob);
//        List<TestValue> justJoe = Arrays.asList(valueJoe);
        List<TestValue> justBob = Arrays.asList(valueBob);
        List<TestValue> neither = Arrays.asList();

        List<TestValue> values0to0 = getTimeCollection().query().byStartTime().afterOrAtTime(0).beforeOrAtTime(0).getList();
        List<TestValue> values1to2 = getTimeCollection().query().byStartTime().afterOrAtTime(1).beforeOrAtTime(2).getList();
        List<TestValue> values2to3 = getTimeCollection().query().byStartTime().afterOrAtTime(2).beforeOrAtTime(3).getList();
        List<TestValue> values3to4 = getTimeCollection().query().byStartTime().afterOrAtTime(3).beforeOrAtTime(4).getList();

        assertEquals(neither, values0to0);
        assertEquals(both, values1to2);
        assertEquals(justBob, values2to3);
        assertEquals(neither, values3to4);
	}
	
	@Test
	public void test_getIterator() throws Exception {
        TestValue valueJoe = new TestValue("Joe");
        TestValue valueBob = new TestValue("Bob");
        insertAtTime(1, valueJoe);
        insertAtTime(2, valueBob);
        Iterator<TestValue> iter;

        iter = getTimeCollection().query().getIterator();
        List<TestValue> list = new ArrayList<>();
        iter.forEachRemaining(list::add);
        assertEquals(2, list.size());
        assertTrue(list.contains(valueJoe));
        assertTrue(list.contains(valueBob));

        iter = getTimeCollection().query().where((v) -> v.getName().equals("Bob")).getIterator();
        List<TestValue> onlyBob = new ArrayList<>();
        iter.forEachRemaining(onlyBob::add);
        assertEquals(1, onlyBob.size());
        assertFalse(onlyBob.contains(valueJoe));
        assertTrue(onlyBob.contains(valueBob));
	}
	
	@Test
	public void test_query_update() throws Exception {
        BlueKey keyJoe   = insertAtTimeFrame(1, 1, new TestValue("Joe", 0));
        BlueKey keyBob   = insertAtTimeFrame(2, 2, new TestValue("Bob", 0));
        BlueKey keyJosey = insertAtTimeFrame(2, 3, new TestValue("Josey", 0));
        BlueKey keyBobby = insertAtTimeFrame(3, 3, new TestValue("Bobby", 0));
        BlueQuery<TestValue> queryForJosey = getTimeCollection().query().afterTime(1).where((v) -> v.getName().startsWith("Jo"));

        // sanity check
        assertCupcakes(keyJoe, 0);
        assertCupcakes(keyBob, 0);
        assertCupcakes(keyJosey, 0);
        assertCupcakes(keyBobby, 0);

        // test update with conditions
        queryForJosey.update((v) -> v.addCupcake());
        assertCupcakes(keyJoe, 0);
        assertCupcakes(keyBob, 0);
        assertCupcakes(keyJosey, 1);
        assertCupcakes(keyBobby, 0);

        // test update all
        getTimeCollection().query().update((v) -> v.addCupcake());
        assertCupcakes(keyJoe, 1);
        assertCupcakes(keyBob, 1);
        assertCupcakes(keyJosey, 2);
        assertCupcakes(keyBobby, 1);

        // test update byStartTime
        getTimeCollection().query().byStartTime().afterOrAtTime(3).update((v) -> v.addCupcake());
        assertCupcakes(keyJoe, 1);
        assertCupcakes(keyBob, 1);
        assertCupcakes(keyJosey, 2);
        assertCupcakes(keyBobby, 2);

        // test replace
        getTimeCollection().query().byStartTime().afterOrAtTime(3).replace((v) -> { return new TestValue(v.getName(), v.getCupcakes() + 2); } );
        assertCupcakes(keyJoe, 1);
        assertCupcakes(keyBob, 1);
        assertCupcakes(keyJosey, 2);
        assertCupcakes(keyBobby, 4);
	}
	
	@Test
	public void test_query_delete() throws Exception {
		TestValue valueJoe = new TestValue("Joe");
		TestValue valueBob = new TestValue("Bob");
		TestValue valueJosey = new TestValue("Josey");
		TestValue valueBobby = new TestValue("Bobby");
		insertAtTimeFrame(1, 1, valueJoe);
		insertAtTimeFrame(2, 2, valueBob);
		insertAtTimeFrame(2, 3, valueJosey);
		insertAtTimeFrame(3, 3, valueBobby);
		List<TestValue> storedValues;
        BlueQuery<TestValue> queryForJosey = getTimeCollection().query().afterTime(1).where((v) -> v.getName().startsWith("Jo"));
        BlueQuery<TestValue> queryByStartTime3 = getTimeCollection().query().byStartTime().afterOrAtTime(3);

        // sanity check
        storedValues = getTimeCollection().query().getList();
        assertEquals(4, storedValues.size());
        assertTrue(storedValues.contains(valueJosey));

        // test if delete works with query conditions
        queryForJosey.delete();
        storedValues = getTimeCollection().query().getList();
        assertEquals(3, storedValues.size());
        assertFalse(storedValues.contains(valueJosey));
        assertTrue(storedValues.contains(valueJoe));

        // test if byStartTime works
        queryByStartTime3.delete();
        storedValues = getTimeCollection().query().getList();
        assertEquals(2, storedValues.size());
        assertFalse(storedValues.contains(valueJosey));
        assertTrue(storedValues.contains(valueJoe));

        // test if delete works without conditions
        getTimeCollection().query().delete();
        storedValues = getTimeCollection().query().getList();
        assertEquals(0, storedValues.size());
	}


	@Test
	public void test_query_update_long() throws Exception {
        BlueKey keyJoe   = insertAtLong(1, new TestValue("Joe", 0));
        BlueKey keyBob   = insertAtLong(2, new TestValue("Bob", 0));
        BlueQuery<TestValue> queryForJoe = getLongCollection().query().where((v) -> v.getName().startsWith("Jo"));

        // sanity check
        assertEquals(0, getLongCollection().get(keyJoe).getCupcakes());
        assertEquals(0, getLongCollection().get(keyBob).getCupcakes());

        // test update with conditions
        queryForJoe.update((v) -> v.addCupcake());
        assertEquals(1, getLongCollection().get(keyJoe).getCupcakes());
        assertEquals(0, getLongCollection().get(keyBob).getCupcakes());

        // test update all
        getLongCollection().query().update((v) -> v.addCupcake());
        assertEquals(2, getLongCollection().get(keyJoe).getCupcakes());
        assertEquals(1, getLongCollection().get(keyBob).getCupcakes());

        // test replace
        queryForJoe.replace((v) -> { return new TestValue(v.getName(), v.getCupcakes() + 2); } );
        assertEquals(4, getLongCollection().get(keyJoe).getCupcakes());
        assertEquals(1, getLongCollection().get(keyBob).getCupcakes());
	}
	
	@Test
	public void test_query_delete_long() throws Exception {
		TestValue valueJoe = new TestValue("Joe");
		TestValue valueBob = new TestValue("Bob");
        insertAtLong(1, valueJoe);
        insertAtLong(2, valueBob);
		List<TestValue> storedValues;
        BlueQuery<TestValue> queryForJoe = getLongCollection().query().where((v) -> v.getName().startsWith("Jo"));

        // sanity check
        storedValues = getLongCollection().query().getList();
        assertEquals(2, storedValues.size());
        assertTrue(storedValues.contains(valueJoe));

        // test if delete works with query conditions
        queryForJoe.delete();
        storedValues = getLongCollection().query().getList();
        assertEquals(1, storedValues.size());
        assertFalse(storedValues.contains(valueJoe));
        assertTrue(storedValues.contains(valueBob));

        // test if delete works without conditions
        getLongCollection().query().delete();
        storedValues = getLongCollection().query().getList();
        assertEquals(0, storedValues.size());
	}

	@Test
	public void test_getAllCollectionsFromDisk() throws Exception {
        getTimeCollection();
        List<ReadOnlyBlueCollectionOnDisk<?>> allCollections = db().getAllCollectionsFromDisk();
        assertEquals(5, allCollections.size());
        db().getCollectionBuilder("string", HashGroupedKey.class, String.class).build();
        db().getCollectionBuilder("long", HashGroupedKey.class, Long.class).build();
        allCollections = db().getAllCollectionsFromDisk();
        assertEquals(7, allCollections.size());
	}

	@Test
	public void test_backup() throws Exception {
        BlueKey key1At1 = createKey(1, 1);
        TestValue value1 = createValue("Anna");
        getTimeCollection().insert(key1At1, value1);

        BlueTimeCollectionOnDisk<TestValue2> secondCollection = (BlueTimeCollectionOnDisk<TestValue2>) db.getTimeCollectionBuilder("testing_2", TimeKey.class, TestValue2.class).build();
        TestValue2 valueInSecondCollection = new TestValue2("Joe", 3);
        secondCollection.insert(key1At1, valueInSecondCollection);

		Path tempFolder = createTempFolder().toPath();
		tempFolder.toFile().deleteOnExit();
		Path backedUpPath = Paths.get(tempFolder.toString(), "backup_test.zip");
		db().backup(backedUpPath);

		Path restoredPath = Paths.get(tempFolder.toString(), "restore_test");
		ZipUtils.extractFiles(backedUpPath, restoredPath);
		Path restoredBlueDbPath = Paths.get(restoredPath.toString(), "bluedb");

		BlueDbOnDisk restoredDb = (BlueDbOnDisk) new BlueDbOnDiskBuilder().withPath(restoredBlueDbPath).build();
        BlueTimeCollectionOnDisk<TestValue> restoredCollection = (BlueTimeCollectionOnDisk<TestValue>) restoredDb.getTimeCollectionBuilder(getTimeCollectionName(), TimeKey.class, TestValue.class).build();
//        BlueCollectionOnDisk<TestValue> restoredCollection = (BlueCollectionOnDisk<TestValue>) restoredDb.initializeCollection(getTimeCollectionName(), TimeKey.class, TestValue.class);
		assertTrue(restoredCollection.contains(key1At1));
		assertEquals(value1, restoredCollection.get(key1At1));

		BlueTimeCollectionOnDisk<TestValue2> secondCollectionRestored = (BlueTimeCollectionOnDisk<TestValue2>) restoredDb.getTimeCollectionBuilder("testing_2", TimeKey.class, TestValue2.class).build();
		assertTrue(secondCollectionRestored.contains(key1At1));
		assertEquals(valueInSecondCollection, secondCollectionRestored.get(key1At1));
	}

	@Test
	public void test_backup_fail() throws Exception {
		@SuppressWarnings("rawtypes")
		BlueTimeCollectionOnDisk newUntypedCollection = (BlueTimeCollectionOnDisk) db.getUntypedCollectionForBackup(getTimeCollectionName());
        Path serializedClassesPath = FileManager.getNewestVersionPath(newUntypedCollection.getPath().resolve(".meta"), CollectionMetaData.FILENAME_SERIALIZED_CLASSES);
        getFileManager().saveObject(serializedClassesPath, "some_nonsense");  // serialize a string where there should be a list

        Path tempFolder = createTempFolder().toPath();
        tempFolder.toFile().deleteOnExit();
        Path backedUpPath = Paths.get(tempFolder.toString(), "backup_test.zip");
        BlueDbOnDisk reopenedDatbase = (BlueDbOnDisk) new BlueDbOnDiskBuilder().withPath(getPath()).build();
        try {
            reopenedDatbase.backup(backedUpPath);
            fail();  // because the "test2" collection was broken, the backup should error out;

        } catch (BlueDbException e) {
        }

	}

	@Test
	public void test_shutdown() throws Exception{
		TestValue testValue = new TestValue("Joe Dirt");
		insertAtTime(10, testValue);
		
		TestTask updateAndSleepTask = new TestTask(() -> {
			Thread.sleep(10);
		});
		
		AsynchronousTestTask queryTask = AsynchronousTestTask.run(() -> {
			getTimeCollection().query()
				.where(value -> testValue.getName().equals(value.getName()))
				.update(value -> {
					updateAndSleepTask.run();
				});
		});
		
		updateAndSleepTask.awaitStart();
		
		db.shutdown();
		validateShutdown();
		
		AsynchronousTestTask interruptedTask = AsynchronousTestTask.run(() -> {
			db.awaitTermination(1, TimeUnit.MINUTES);
		});
		interruptedTask.interrupt();
		
		assertEquals(false, db.awaitTermination(3, TimeUnit.MILLISECONDS));
		assertEquals(true, db.awaitTermination(1, TimeUnit.MINUTES));
		TestUtils.assertThrowable(BlueDbException.class, interruptedTask.getError());
		
		assertEquals(true, queryTask.isComplete());
		assertEquals(true, queryTask.getError() == null);
		
		assertEquals(true, updateAndSleepTask.isComplete());
		assertEquals(true, updateAndSleepTask.getError() == null);
	}

	@Test
	public void test_shutdownNow() throws Exception {
		TestValue testValue = new TestValue("Joe Dirt");
		insertAtTime(10, testValue);
		
		TestTask updateAndSleepTask = new TestTask(() -> {
			Thread.sleep(100);
		});
		
		AsynchronousTestTask queryTask = AsynchronousTestTask.run(() -> {
			getTimeCollection().query()
				.where(value -> testValue.getName().equals(value.getName()))
				.update(value -> {
					updateAndSleepTask.run();
				});
		});
		
		updateAndSleepTask.awaitStart();
		
		db.shutdownNow();
		validateShutdown();
		
		assertEquals(true, db.awaitTermination(1, TimeUnit.MINUTES));
		
		assertEquals(true, queryTask.isComplete());
		TestUtils.assertThrowable(null, queryTask.getError()); //query is run on separate thread, so we won't see the intruption on this task
		
		assertEquals(true, updateAndSleepTask.isComplete());
		TestUtils.assertThrowable(InterruptedException.class, updateAndSleepTask.getError());
	}

	private void validateShutdown() {
		TestTask scheduledTask = TestTask.run(() -> {
			db.getSharedExecutor().scheduleTaskAtFixedRate(() -> System.out.println("Hi!"), 10, 10, TimeUnit.MINUTES);
		});
		TestTask queryTask = TestTask.run(() -> {
			insertAtTime(11, new TestValue("Oliver"));
		});
	
		TestUtils.assertThrowable(RejectedExecutionException.class, scheduledTask.getError());
		TestUtils.assertThrowable(RejectedExecutionException.class, queryTask.getError());
	}
}
