package io.bluedb.disk;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import io.bluedb.api.BlueCollection;
import io.bluedb.api.BlueQuery;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.api.keys.ValueKey;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.zip.ZipUtils;

public class BlueDbOnDiskTest extends BlueDbDiskTestBase {

	@Test
	public void test_shutdown() throws Exception{
		db.shutdown();
	}

    @Test
    public void test_getUntypedCollectionForBackup() throws Exception {
        String timeCollectionName = getTimeCollectionName();
        BlueCollectionOnDisk<String> newCollection = (BlueCollectionOnDisk<String>) db.initializeCollection("new_collection", TimeKey.class, String.class);
        assertNotNull(db.getUntypedCollectionForBackup(timeCollectionName));
        assertNotNull(db.getUntypedCollectionForBackup("new_collection"));

        // now let's break the new collection
        Path newCollectionPath = newCollection.getPath();
        Path serializedClassesPath = Paths.get(newCollectionPath.toString(), ".meta", "serialized_classes");
        getFileManager().saveObject(serializedClassesPath, "some_nonsense");  // serialize a string where there should be a list


        BlueDbOnDisk reopenedDatbase = new BlueDbOnDiskBuilder().setPath(getPath()).build();
        assertNotNull(reopenedDatbase.getUntypedCollectionForBackup(timeCollectionName));  // this one isn't broken
		try {
			reopenedDatbase.getUntypedCollectionForBackup("new_collection"); // we broke it above
			fail();
		} catch (Throwable e) {
		}
	}

	@Test
	public void test_getCollection() throws Exception {
		db.initializeCollection(getTimeCollectionName(), TimeKey.class, TestValue.class);
		BlueCollection<TestValue> collection = db.getCollection(getTimeCollectionName(), TestValue.class);
		assertNotNull(collection);
		assertEquals(collection, db.getCollection(getTimeCollectionName(), TestValue.class));
		assertNull(db.getCollection("non-existing", TestValue.class));
	}

	@Test
	public void test_getCollection_wrong_type() throws Exception {
		BlueCollection<TestValue> valueCollection = db.initializeCollection(getTimeCollectionName(), TimeKey.class, TestValue.class);
		TimeKey testValueKey = new TimeKey(1, 1);
		valueCollection.insert(testValueKey, new TestValue("Bob"));
		try {
			db.getCollection(getTimeCollectionName(), String.class);
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_initializeCollection_existing_correct_type() throws Exception {
		db.initializeCollection(getTimeCollectionName(), TimeKey.class, TestValue.class);
		assertNotNull(db.initializeCollection(getTimeCollectionName(), TimeKey.class, TestValue.class));  // make sure it works the second time as well
	}

	@Test
	public void test_initializeCollection_invalid_type() {
		insertAtTime(10, new TestValue("Bob"));
		try {
			db.initializeCollection(getTimeCollectionName(), TimeKey.class, TestValue2.class);
			fail();
		} catch(BlueDbException e) {
		}
	}

	@Test
	public void test_initializeCollection_invalid_key_type() {
		try {
			db.initializeCollection(getTimeCollectionName(), ValueKey.class, TestValue.class);
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
	}

	@Test
	public void test_query_AfterTime_timeframe() throws Exception {
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
	}

	@Test
	public void test_query_AfterTime() throws Exception {
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
	}

	@Test
	public void test_query_AfterOrAtTime_timeframe() throws Exception {
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
	}

	@Test
	public void test_query_AfterOrAtTime() throws Exception {
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
	}
	
	@Test
	public void test_query_update() throws Exception {
        BlueKey keyJoe   = insertAtTime(1, new TestValue("Joe", 0));
        BlueKey keyBob   = insertAtTime(2, new TestValue("Bob", 0));
        BlueKey keyJosey = insertAtTime(2,  new TestValue("Josey", 0));
        BlueKey keyBobby = insertAtTime(3, new TestValue("Bobby", 0));
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
	}
	
	@Test
	public void test_query_delete() throws Exception {
		TestValue valueJoe = new TestValue("Joe");
		TestValue valueBob = new TestValue("Bob");
		TestValue valueJosey = new TestValue("Josey");
		TestValue valueBobby = new TestValue("Bobby");
		insertAtTime(1, valueJoe);
		insertAtTime(2, valueBob);
		insertAtTime(2, valueJosey);
		insertAtTime(3, valueBobby);
		List<TestValue> storedValues;
        BlueQuery<TestValue> queryForJosey = getTimeCollection().query().afterTime(1).where((v) -> v.getName().startsWith("Jo"));

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

        // test if delete works without conditions
        getTimeCollection().query().delete();
        storedValues = getTimeCollection().query().getList();
        assertEquals(0, storedValues.size());
	}

	@Test
	public void test_getAllCollectionsFromDisk() throws Exception {
        getTimeCollection();
        List<BlueCollectionOnDisk<?>> allCollections = db().getAllCollectionsFromDisk();
        assertEquals(2, allCollections.size());
        db().initializeCollection("string", ValueKey.class, String.class);
        db().initializeCollection("long", ValueKey.class, Long.class);
        allCollections = db().getAllCollectionsFromDisk();
        assertEquals(4, allCollections.size());
	}

	@Test
	public void test_backup() throws Exception {
        BlueKey key1At1 = createKey(1, 1);
        TestValue value1 = createValue("Anna");
        getTimeCollection().insert(key1At1, value1);

        BlueCollectionOnDisk<TestValue2> secondCollection = (BlueCollectionOnDisk<TestValue2>) db().initializeCollection("testing_2", TimeKey.class, TestValue2.class);
        TestValue2 valueInSecondCollection = new TestValue2("Joe", 3);
        secondCollection.insert(key1At1, valueInSecondCollection);

		Path tempFolder = createTempFolder().toPath();
		tempFolder.toFile().deleteOnExit();
		Path backedUpPath = Paths.get(tempFolder.toString(), "backup_test.zip");
		db().backup(backedUpPath);

		Path restoredPath = Paths.get(tempFolder.toString(), "restore_test");
		ZipUtils.extractFiles(backedUpPath, restoredPath);
		Path restoredBlueDbPath = Paths.get(restoredPath.toString(), "bluedb");

		BlueDbOnDisk restoredDb = new BlueDbOnDiskBuilder().setPath(restoredBlueDbPath).build();
        BlueCollectionOnDisk<TestValue> restoredCollection = (BlueCollectionOnDisk<TestValue>) restoredDb.initializeCollection(getTimeCollectionName(), TimeKey.class, TestValue.class);
		assertTrue(restoredCollection.contains(key1At1));
		assertEquals(value1, restoredCollection.get(key1At1));
		Long restoredMaxLong = restoredCollection.getMaxLongId();
		assertNotNull(restoredMaxLong);
        assertEquals(getTimeCollection().getMaxLongId().longValue(), restoredMaxLong.longValue());

        BlueCollectionOnDisk<TestValue2> secondCollectionRestored = (BlueCollectionOnDisk<TestValue2>) restoredDb.initializeCollection("testing_2", TimeKey.class, TestValue2.class);
		assertTrue(secondCollectionRestored.contains(key1At1));
		assertEquals(valueInSecondCollection, secondCollectionRestored.get(key1At1));
	}

	@Test
	public void test_backup_fail() throws Exception {
		@SuppressWarnings("rawtypes")
        BlueCollectionOnDisk newUntypedCollection = db.getUntypedCollectionForBackup(getTimeCollectionName());
        Path serializedClassesPath = Paths.get(newUntypedCollection.getPath().toString(), ".meta", "serialized_classes");
        getFileManager().saveObject(serializedClassesPath, "some_nonsense");  // serialize a string where there should be a list

        Path tempFolder = createTempFolder().toPath();
        tempFolder.toFile().deleteOnExit();
        Path backedUpPath = Paths.get(tempFolder.toString(), "backup_test.zip");
        BlueDbOnDisk reopenedDatbase = new BlueDbOnDiskBuilder().setPath(getPath()).build();
        try {
            reopenedDatbase.backup(backedUpPath);
            fail();  // because the "test2" collection was broken, the backup should error out;

        } catch (BlueDbException e) {
        }

	}
}
