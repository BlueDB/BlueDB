package org.bluedb.disk;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.bluedb.TestUtils;
import org.bluedb.api.BlueCollection;
import org.bluedb.api.BlueQuery;
import org.bluedb.api.Updater;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.HashGroupedKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.collection.ReadWriteTimeCollectionOnDisk;
import org.bluedb.disk.collection.metadata.ReadWriteCollectionMetaData;
import org.bluedb.disk.file.FileUtils;
import org.bluedb.disk.file.ReadWriteFileManager;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.recovery.PendingBatchChange;
import org.bluedb.disk.recovery.PendingChange;
import org.bluedb.disk.recovery.PendingRollup;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.rollup.RollupTarget;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.tasks.AsynchronousTestTask;
import org.bluedb.tasks.TestTask;
import org.bluedb.zip.ZipUtils;
import org.junit.Test;

public class ReadableDbOnDiskTest extends BlueDbDiskTestBase {

	@Test
    public void test_getUntypedCollectionForBackup() throws Exception {
        String timeCollectionName = getTimeCollectionName();
		ReadWriteCollectionOnDisk<String> newCollection = (ReadWriteCollectionOnDisk<String>) db.getCollectionBuilder("new_collection", TimeKey.class, String.class).build();
        assertNotNull(db.getUntypedCollectionForBackup(timeCollectionName));
        assertNotNull(db.getUntypedCollectionForBackup("new_collection"));

        // now let's break the new collection
        Path newCollectionPath = newCollection.getPath();
        Path serializedClassesPath = Paths.get(newCollectionPath.toString(), ".meta", "serialized_classes");
        getFileManager().saveObject(serializedClassesPath, "some_nonsense");  // serialize a string where there should be a list


        ReadWriteDbOnDisk reopenedDatbase = (ReadWriteDbOnDisk) new BlueDbOnDiskBuilder().withPath(getPath()).build();
        assertNotNull(reopenedDatbase.getUntypedCollectionForBackup(timeCollectionName));  // this one isn't broken
		try {
			reopenedDatbase.getUntypedCollectionForBackup("new_collection"); // we broke it above
			fail();
		} catch (Throwable e) {
		}
	}
    //This test we want to pass into getUntypedCollectionForBackup a string for a filename which does not contain a .meta key_type file
    //Currently it is causing a null pointer when this happens
	@Test
    public void test_getUntypedCollectionForBackup_withInvalidPath() throws BlueDbException {
        assertNull(db.getUntypedCollectionForBackup("bad_folder"));
    }

	@SuppressWarnings("deprecation")
	@Test
	public void test_getCollection() throws Exception {
		// NOTE: we need at least one use of this deprecated method to maintain 100% coverage.
		db.collectionBuilder(getTimeCollectionName(), TimeKey.class, TestValue.class).build();

		BlueCollection<TestValue> collection = db.getTimeCollection(getTimeCollectionName(), TestValue.class);
		assertNotNull(collection);
		assertEquals(collection, db.getCollection(getTimeCollectionName(), TestValue.class));
		assertNull(db.getCollection("non-existing", TestValue.class));
	}

	@Test
	public void test_getCollection_noSegmentSizeMetaData() throws Exception {
		Path segmentSizePath = db.getPath().resolve("testing_time/.meta/segment_size");
		Files.delete(segmentSizePath);
		
		ReadWriteDbOnDisk newDb = (ReadWriteDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db.getPath()).build();
		newDb.getTimeCollectionBuilder(getTimeCollectionName(), TimeKey.class, TestValue.class).build();
		assertTrue(FileUtils.exists(segmentSizePath));
	}

	@Test
	public void test_getCollection_wrong_type() throws Exception {
		BlueCollection<TestValue> valueCollection = db.getCollectionBuilder(getTimeCollectionName(), TimeKey.class, TestValue.class).build();
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
		db.getCollectionBuilder(getTimeCollectionName(), TimeKey.class, TestValue.class).build();
		assertNotNull(db.getCollectionBuilder(getTimeCollectionName(), TimeKey.class, TestValue.class).build());  // make sure it works the second time as well
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
	public void test_initializeTimeCollection_invalid_type() {
		insertAtTime(10, new TestValue("Bob"));
		try {
			db.initializeTimeCollection(getTimeCollectionName(), TimeKey.class, TestValue2.class, Arrays.asList(), null);
			fail();
		} catch(BlueDbException e) {
		}
	}

	@Test
	public void test_initializeTimeCollection_invalid_key_type() {
		try {
			db.initializeTimeCollection(getTimeCollectionName(), HashGroupedKey.class, TestValue.class, Arrays.asList(), null);
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

        final Iterator<TestValue> autoCloseFastIterator = getTimeCollection().query().where((v) -> v.getName().equals("Bob")).getIterator(10, TimeUnit.MILLISECONDS);
        Blutils.trySleep(15);
        TestTask useIteratorAfterAutoClosedTask = new TestTask(() -> {
        	autoCloseFastIterator.hasNext();
        });
        useIteratorAfterAutoClosedTask.run();
        TestUtils.assertThrowable(RuntimeException.class, useIteratorAfterAutoClosedTask.getError());
        
        final Iterator<TestValue> autoCloseAfterOneSecondIterator = getTimeCollection().query().getIterator(1, TimeUnit.SECONDS);
        assertTrue(autoCloseAfterOneSecondIterator.hasNext());
        Blutils.trySleep(750);
        assertTrue(autoCloseAfterOneSecondIterator.hasNext());
        Blutils.trySleep(1500);
        useIteratorAfterAutoClosedTask = new TestTask(() -> {
        	autoCloseAfterOneSecondIterator.hasNext();
        });
        useIteratorAfterAutoClosedTask.run();
        TestUtils.assertThrowable(RuntimeException.class, useIteratorAfterAutoClosedTask.getError());
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
        List<ReadWriteCollectionOnDisk<?>> allCollections = db().getAllCollectionsFromDisk();
        assertEquals(5, allCollections.size());
        db().getCollectionBuilder("string", HashGroupedKey.class, String.class).build();
        db().getCollectionBuilder("long", HashGroupedKey.class, Long.class).build();
        allCollections = db().getAllCollectionsFromDisk();
        assertEquals(7, allCollections.size());
	}

	@Test
    public void test_getAllCollectionsFromDiskWithNullKeyType() throws Exception {
	    getTimeCollection();
        List<ReadWriteCollectionOnDisk<?>> allCollections = db().getAllCollectionsFromDisk();
        assertEquals(5, allCollections.size());
        db().getCollectionBuilder("null_key", null, String.class).build();
        allCollections = db().getAllCollectionsFromDisk();
        assertEquals(5, allCollections.size());
    }

	@Test
	public void test_backup() throws Exception {
        BlueKey key1At1 = createKey(1, 1);
        TestValue value1 = createValue("Anna");
        getTimeCollection().insert(key1At1, value1);

        ReadWriteTimeCollectionOnDisk<TestValue2> secondCollection = (ReadWriteTimeCollectionOnDisk<TestValue2>) db.getTimeCollectionBuilder("testing_2", TimeKey.class, TestValue2.class).build();
        TestValue2 valueInSecondCollection = new TestValue2("Joe", 3);
        secondCollection.insert(key1At1, valueInSecondCollection);

		Path tempFolder = createTempFolder().toPath();
		tempFolder.toFile().deleteOnExit();
		Path backedUpPath = Paths.get(tempFolder.toString(), "backup_test.zip");
		db().backup(backedUpPath);

		Path restoredPath = Paths.get(tempFolder.toString(), "restore_test");
		ZipUtils.extractFiles(backedUpPath, restoredPath);
		Path restoredBlueDbPath = Paths.get(restoredPath.toString(), "bluedb");

		ReadWriteDbOnDisk restoredDb = (ReadWriteDbOnDisk) new BlueDbOnDiskBuilder().withPath(restoredBlueDbPath).build();
        ReadWriteTimeCollectionOnDisk<TestValue> restoredCollection = (ReadWriteTimeCollectionOnDisk<TestValue>) restoredDb.getTimeCollectionBuilder(getTimeCollectionName(), TimeKey.class, TestValue.class).build();
//        BlueCollectionOnDisk<TestValue> restoredCollection = (BlueCollectionOnDisk<TestValue>) restoredDb.initializeCollection(getTimeCollectionName(), TimeKey.class, TestValue.class);
		assertTrue(restoredCollection.contains(key1At1));
		assertEquals(value1, restoredCollection.get(key1At1));

		ReadWriteTimeCollectionOnDisk<TestValue2> secondCollectionRestored = (ReadWriteTimeCollectionOnDisk<TestValue2>) restoredDb.getTimeCollectionBuilder("testing_2", TimeKey.class, TestValue2.class).build();
		assertTrue(secondCollectionRestored.contains(key1At1));
		assertEquals(valueInSecondCollection, secondCollectionRestored.get(key1At1));
	}

	@Test
	public void test_backup_fail() throws Exception {
		@SuppressWarnings("rawtypes")
		ReadWriteTimeCollectionOnDisk newUntypedCollection = (ReadWriteTimeCollectionOnDisk) db.getUntypedCollectionForBackup(getTimeCollectionName());
        Path serializedClassesPath = ReadWriteFileManager.getNewestVersionPath(newUntypedCollection.getPath().resolve(".meta"), ReadWriteCollectionMetaData.FILENAME_SERIALIZED_CLASSES);
        getFileManager().saveObject(serializedClassesPath, "some_nonsense");  // serialize a string where there should be a list

        Path tempFolder = createTempFolder().toPath();
        tempFolder.toFile().deleteOnExit();
        Path backedUpPath = Paths.get(tempFolder.toString(), "backup_test.zip");
        ReadWriteDbOnDisk reopenedDatbase = (ReadWriteDbOnDisk) new BlueDbOnDiskBuilder().withPath(getPath()).build();
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
	public void test_assertExistingCollectionIsType() throws BlueDbException {
		ReadWriteDbOnDisk.assertExistingCollectionIsType(getTimeCollection(), ReadWriteTimeCollectionOnDisk.class);
		try {
			ReadWriteDbOnDisk.assertExistingCollectionIsType(getLongCollection(), ReadWriteTimeCollectionOnDisk.class);
			fail();
		} catch (BlueDbException e) {
		}
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

	@Test
	public void test_backupTimeFrame_fail() throws Exception {
		@SuppressWarnings("rawtypes")
		ReadWriteTimeCollectionOnDisk newUntypedCollection = (ReadWriteTimeCollectionOnDisk) db.getUntypedCollectionForBackup(getTimeCollectionName());
        Path serializedClassesPath = ReadWriteFileManager.getNewestVersionPath(newUntypedCollection.getPath().resolve(".meta"), ReadWriteCollectionMetaData.FILENAME_SERIALIZED_CLASSES);
        getFileManager().saveObject(serializedClassesPath, "some_nonsense");  // serialize a string where there should be a list

        Path tempFolder = createTempFolder().toPath();
        tempFolder.toFile().deleteOnExit();
        Path backedUpPath = Paths.get(tempFolder.toString(), "backup_test.zip");
        ReadWriteDbOnDisk reopenedDatbase = (ReadWriteDbOnDisk) new BlueDbOnDiskBuilder().withPath(getPath()).build();
        try {
            reopenedDatbase.backupTimeFrame(backedUpPath, Long.MIN_VALUE, Long.MAX_VALUE);
            fail();  // because the "test2" collection was broken, the backup should error out;

        } catch (BlueDbException e) {
        }

	}

	@Test
	public void test_backupTimeFrame() throws Exception {
		String timeFrameCollectionName = "testing_2";
		
		long millisInSegment = getTimeCollectionMetaData().getSegmentSize().getSegmentSize();
		
		//Segment A Times
		long a1 = 1_000;
		long a2 = millisInSegment - 1_000;
		
		//Segment B Times
		long b1 = millisInSegment + 1_000;
		long b2 = b1 + (10_000 * 2);
		long b3 = b1 + (10_000 * 3);
		long b4 = (millisInSegment * 2) - (10_000 * 3);
		long b5 = (millisInSegment * 2) - (10_000 * 2);
		long b6 = (millisInSegment * 2) - 1_000;
		
		//Segment C Times
		long c1 = (millisInSegment * 2) + 1_000;
		long c2 = (millisInSegment * 3) - 1_000;
		
		long backupStart = b2;
		long backupEnd = b5;
		
		List<SelectiveBackupItem> timeItems = createTimeItems(a1, b1, b2, b3, b5, b6, c1);
		insertItems(timeItems, timeCollection);
		
		ReadWriteTimeCollectionOnDisk<TestValue> secondCollection = (ReadWriteTimeCollectionOnDisk<TestValue>) db.getTimeCollectionBuilder(timeFrameCollectionName, TimeFrameKey.class, TestValue.class).build();
		List<SelectiveBackupItem> timeFrameItems = createTimeFrameItems(a1, a2, b1, b3, b4, b6, c1, c2);
		List<SelectiveBackupItem> excludedTimeFrameItems = timeFrameItems.stream()
				.filter(value -> !value.shouldBeIncluded)
				.collect(Collectors.toList());
				
		insertItems(timeFrameItems, secondCollection);
		
		//Force rollups so that we can test the querying of chunks that contain multiple items.
		timeCollection.getRollupScheduler().forceScheduleRollups();
		secondCollection.getRollupScheduler().forceScheduleRollups();
		
		/*
		 * Run updates on each collection in order to wait until rollups are done. These both update the cupcake
		 * count of a single excluded value so it shouldn't affect our expectations when it is time to assert
		 * the restored data.
		 */
		timeCollection.update(timeItems.get(timeItems.size()-1).getKey(), value -> value.addCupcake());
		secondCollection.update(timeFrameItems.get(timeFrameItems.size()-1).getKey(), value -> value.addCupcake());
		
		Path tempFolder = createTempFolder().toPath();
		tempFolder.toFile().deleteOnExit();
		
		backupRestoreAndValidateResults(timeFrameCollectionName, backupStart, backupEnd, timeItems, timeFrameItems, tempFolder);
		
		backupRestoreAndValidateWithPendingChangesIncluded(timeFrameCollectionName, backupStart, backupEnd, timeItems, secondCollection, timeFrameItems, excludedTimeFrameItems, tempFolder);
	}

	private List<SelectiveBackupItem> createTimeItems(long a1, long b1, long b2, long b3, long b5, long b6, long c1) {
		List<SelectiveBackupItem> timeItems = new LinkedList<>();
		
		TimeKey excludedValue1Key = createKey(1, a1);
		TestValue excludedValue1 = new TestValue("excludedValue1");
		timeItems.add(new SelectiveBackupItem(excludedValue1Key, excludedValue1, false));
		
		TimeKey excludedValue2Key = createKey(2, b1);
		TestValue excludedValue2 = new TestValue("excludedValue2");
		timeItems.add(new SelectiveBackupItem(excludedValue2Key, excludedValue2, false));
		
		TimeKey includedValue1Key = createKey(3, b2);
		TestValue includedValue1 = new TestValue("includedValue1");
		timeItems.add(new SelectiveBackupItem(includedValue1Key, includedValue1, true));
		
		TimeKey includedValue2Key = createKey(4, b3);
		TestValue includedValue2 = new TestValue("includedValue2");
		timeItems.add(new SelectiveBackupItem(includedValue2Key, includedValue2, true));
		
		TimeKey includedValue3Key = createKey(5, b5);
		TestValue includedValue3 = new TestValue("includedValue3");
		timeItems.add(new SelectiveBackupItem(includedValue3Key, includedValue3, true));
		
		TimeKey excludedValue3Key = createKey(6, b6);
		TestValue excludedValue3 = new TestValue("excludedValue3");
		timeItems.add(new SelectiveBackupItem(excludedValue3Key, excludedValue3, false));
		
		TimeKey excludedValue4Key = createKey(7, c1);
		TestValue excludedValue4 = new TestValue("excludedValue4");
		timeItems.add(new SelectiveBackupItem(excludedValue4Key, excludedValue4, false));
		return timeItems;
	}

	private List<SelectiveBackupItem> createTimeFrameItems(long a1, long a2, long b1, long b3, long b4, long b6, long c1, long c2) {
		List<SelectiveBackupItem> timeFrameItems = new LinkedList<>();
		
		TestValue excludedFrameValue1 = new TestValue("excludedFrameValue1");
		TimeFrameKey excludedFrameValue1Key = createTimeFrameKey(a1, a2, excludedFrameValue1);
		timeFrameItems.add(new SelectiveBackupItem(excludedFrameValue1Key, excludedFrameValue1, false));
		
		TestValue excludedFrameValue2 = new TestValue("excludedFrameValue2");
		TimeFrameKey excludedFrameValue2Key = createTimeFrameKey(a2, b1, excludedFrameValue2);
		timeFrameItems.add(new SelectiveBackupItem(excludedFrameValue2Key, excludedFrameValue2, false));
		
		TestValue includedFrameValue1 = new TestValue("includedFrameValue1");
		TimeFrameKey includedFrameValue1Key = createTimeFrameKey(a2, b3, includedFrameValue1);
		timeFrameItems.add(new SelectiveBackupItem(includedFrameValue1Key, includedFrameValue1, true));
		
		TestValue includedFrameValue2 = new TestValue("includedFrameValue2");
		TimeFrameKey includedFrameValue2Key = createTimeFrameKey(a2, b6, includedFrameValue2);
		timeFrameItems.add(new SelectiveBackupItem(includedFrameValue2Key, includedFrameValue2, true));
		
		TestValue includedFrameValue3 = new TestValue("includedFrameValue3");
		TimeFrameKey includedFrameValue3Key = createTimeFrameKey(a2, c1, includedFrameValue3);
		timeFrameItems.add(new SelectiveBackupItem(includedFrameValue3Key, includedFrameValue3, true));
		
		TestValue includedFrameValue4 = new TestValue("includedFrameValue4");
		TimeFrameKey includedFrameValue4Key = createTimeFrameKey(b1, b4, includedFrameValue4);
		timeFrameItems.add(new SelectiveBackupItem(includedFrameValue4Key, includedFrameValue4, true));
		
		TestValue includedFrameValue5 = new TestValue("includedFrameValue5");
		TimeFrameKey includedFrameValue5Key = createTimeFrameKey(b1, b6, includedFrameValue5);
		timeFrameItems.add(new SelectiveBackupItem(includedFrameValue5Key, includedFrameValue5, true));
		
		TestValue includedFrameValue6 = new TestValue("includedFrameValue6");
		TimeFrameKey includedFrameValue6Key = createTimeFrameKey(b3, b4, includedFrameValue6);
		timeFrameItems.add(new SelectiveBackupItem(includedFrameValue6Key, includedFrameValue6, true));
		
		TestValue includedFrameValue7 = new TestValue("includedFrameValue7");
		TimeFrameKey includedFrameValue7Key = createTimeFrameKey(b3, b6, includedFrameValue7);
		timeFrameItems.add(new SelectiveBackupItem(includedFrameValue7Key, includedFrameValue7, true));
		
		TestValue includedFrameValue8 = new TestValue("includedFrameValue8");
		TimeFrameKey includedFrameValue8Key = createTimeFrameKey(b3, c1, includedFrameValue8);
		timeFrameItems.add(new SelectiveBackupItem(includedFrameValue8Key, includedFrameValue8, true));
		
		TestValue excludedFrameValue3 = new TestValue("excludedFrameValue3");
		TimeFrameKey excludedFrameValue3Key = createTimeFrameKey(b6, c1, excludedFrameValue3);
		timeFrameItems.add(new SelectiveBackupItem(excludedFrameValue3Key, excludedFrameValue3, false));
		
		TestValue excludedFrameValue4 = new TestValue("excludedFrameValue4");
		TimeFrameKey excludedFrameValue4Key = createTimeFrameKey(c1, c2, excludedFrameValue4);
		timeFrameItems.add(new SelectiveBackupItem(excludedFrameValue4Key, excludedFrameValue4, false));
		return timeFrameItems;
	}

	private void insertItems(List<SelectiveBackupItem> timeItems, ReadWriteTimeCollectionOnDisk<TestValue> collection) throws BlueDbException {
		for(SelectiveBackupItem item : timeItems) {
			collection.insert(item.getKey(), item.getValue());
        }
	}

	private void backupRestoreAndValidateResults(String timeFrameCollectionName, long backupStart, long backupEnd, List<SelectiveBackupItem> timeItems, List<SelectiveBackupItem> timeFrameItems, Path tempFolder) throws BlueDbException, IOException {
		Path backedUpPath = Paths.get(tempFolder.toString(), "backup_test1.zip");
		backedUpPath.toFile().deleteOnExit();
		db().backupTimeFrame(backedUpPath, backupStart, backupEnd);
		
		Path restoredPath = Paths.get(tempFolder.toString(), "restore_test1");
		ZipUtils.extractFiles(backedUpPath, restoredPath);
		Path restoredBlueDbPath = Paths.get(restoredPath.toString(), "bluedb");

		ReadWriteDbOnDisk restoredDb = (ReadWriteDbOnDisk) new BlueDbOnDiskBuilder().withPath(restoredBlueDbPath).build();
        ReadWriteTimeCollectionOnDisk<TestValue> restoredCollection1 = (ReadWriteTimeCollectionOnDisk<TestValue>) restoredDb.getTimeCollectionBuilder(getTimeCollectionName(), TimeKey.class, TestValue.class).build();
        ReadWriteTimeCollectionOnDisk<TestValue> restoredCollection2 = (ReadWriteTimeCollectionOnDisk<TestValue>) restoredDb.getTimeCollectionBuilder(timeFrameCollectionName, TimeFrameKey.class, TestValue.class).build();
        
        validateItems(timeItems, restoredCollection1);
        validateItems(timeFrameItems, restoredCollection2);
	}

	private void backupRestoreAndValidateWithPendingChangesIncluded(String timeFrameCollectionName, long backupStart,
			long backupEnd, List<SelectiveBackupItem> timeItems,
			ReadWriteTimeCollectionOnDisk<TestValue> secondCollection, List<SelectiveBackupItem> timeFrameItems,
			List<SelectiveBackupItem> excludedTimeFrameItems, Path tempFolder) throws BlueDbException, IOException {
		Path backedUpPath = Paths.get(tempFolder.toString(), "backup_test2.zip");
		backedUpPath.toFile().deleteOnExit();
		
		createPendingChangeFiles(timeCollection, timeItems);
		createPendingChangeBatchFile(secondCollection, timeFrameItems);
		createPendingChangeBatchFile(secondCollection, excludedTimeFrameItems);
		createPendingRollupFile();
		
		List<ReadWriteCollectionOnDisk<?>> collectionsToBackup = db().getAllCollectionsFromDisk();
		db().backupManager.backup(collectionsToBackup, backedUpPath, new Range(backupStart, backupEnd), true);

		Path restoredPath = Paths.get(tempFolder.toString(), "restore_test2");
		ZipUtils.extractFiles(backedUpPath, restoredPath);
		Path restoredBlueDbPath = Paths.get(restoredPath.toString(), "bluedb");

		ReadWriteDbOnDisk restoredDb = (ReadWriteDbOnDisk) new BlueDbOnDiskBuilder().withPath(restoredBlueDbPath).build();
        ReadWriteTimeCollectionOnDisk<TestValue> restoredCollection1 = (ReadWriteTimeCollectionOnDisk<TestValue>) restoredDb.getTimeCollectionBuilder(getTimeCollectionName(), TimeKey.class, TestValue.class).build();
        ReadWriteTimeCollectionOnDisk<TestValue> restoredCollection2 = (ReadWriteTimeCollectionOnDisk<TestValue>) restoredDb.getTimeCollectionBuilder(timeFrameCollectionName, TimeFrameKey.class, TestValue.class).build();
        
        /*
         * Set the expected cupcake counts one higher since the pending changes which add one cupcake should
         * be executed on startup. 
         */
        timeItems.stream().forEach(item -> item.getValue().addCupcake());
        timeFrameItems.stream().forEach(item -> item.getValue().addCupcake());
        
        validateItems(timeItems, restoredCollection1);
        validateItems(timeFrameItems, restoredCollection2);
	}

	private void createPendingChangeFiles(ReadWriteTimeCollectionOnDisk<TestValue> collection, List<SelectiveBackupItem> items) throws BlueDbException {
		BlueSerializer serializer = collection.getSerializer();
		
		for(SelectiveBackupItem item : items) {
			Updater<TestValue> updater = value -> value.addCupcake();
			PendingChange<TestValue> change = PendingChange.createUpdate(item.getKey(), item.getValue(), updater, serializer);
			collection.getRecoveryManager().saveChange(change);
        }
	}

	private void createPendingChangeBatchFile(ReadWriteTimeCollectionOnDisk<TestValue> collection, List<SelectiveBackupItem> items) throws BlueDbException {
		BlueSerializer serializer = collection.getSerializer();
		
		List<IndividualChange<TestValue>> changesForBatch = new LinkedList<>();
		
		for(SelectiveBackupItem item : items) {
			TestValue updatedValue = serializer.clone(item.getValue());
			updatedValue.addCupcake();
			changesForBatch.add(IndividualChange.createUpdateChange(item.getKey(), item.getValue(), updatedValue));
        }
		Collections.sort(changesForBatch);
		
		PendingBatchChange<TestValue> batchChange = PendingBatchChange.createBatchChange(changesForBatch);
		collection.getRecoveryManager().saveChange(batchChange);
	}

	private void createPendingRollupFile() throws BlueDbException {
		RollupTarget rollupTarget = new RollupTarget(0, timeCollection.getSegmentManager().getSegment(0).getRange());
		PendingRollup<TestValue> rollupTask = new PendingRollup<>(rollupTarget);
		timeCollection.getRecoveryManager().saveChange(rollupTask);
	}

	private void validateItems(List<SelectiveBackupItem> items, ReadWriteTimeCollectionOnDisk<TestValue> collection) throws BlueDbException {
		for(SelectiveBackupItem item : items) {
			String errorMessage1 = "Unexpected Contains " + item.key;
			assertEquals(errorMessage1, item.shouldBeIncluded(), collection.contains(item.getKey()));
        	if(item.shouldBeIncluded()) {
        		TestValue restoredValue = collection.get(item.getKey());
				String errorMessage2 = "Original Value does NOT equal restored value [original]" + item.getValue() + " [restored]" + restoredValue;
				assertEquals(errorMessage2, item.getValue(), restoredValue);
        	}
        }
	}
	
	private static class SelectiveBackupItem {
		private BlueKey key;
		private TestValue value;
		private boolean shouldBeIncluded;
		
		public SelectiveBackupItem(BlueKey key, TestValue value, boolean shouldBeIncluded) {
			this.key = key;
			this.value = value;
			this.shouldBeIncluded = shouldBeIncluded;
		}
		
		public BlueKey getKey() {
			return key;
		}
		
		public TestValue getValue() {
			return value;
		}
		
		public boolean shouldBeIncluded() {
			return shouldBeIncluded;
		}
	}
}
