package io.bluedb.disk;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import io.bluedb.api.BlueQuery;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.zip.ZipUtils;

public class BlueDbOnDiskTest extends BlueDbDiskTestBase {

	@Test
	public void test_shutdown() throws Exception{
		db.shutdown();
	}

	@Test
	public void test_getCollection_untyped() throws Exception {
		assertNotNull(db.getCollection("testing1"));
		assertNotNull(db.getCollection("testing2"));  // this time it should create the collection

		@SuppressWarnings("rawtypes")
		BlueCollectionOnDisk newUntypedCollection = db.getCollection("testing2"); // make sure it's created
		Path serializedClassesPath = Paths.get(newUntypedCollection.getPath().toString(), ".meta", "serialized_classes");
		getFileManager().saveObject(serializedClassesPath, "some_nonsense");  // serialize a string where there should be a list

		try {
			db.getCollection("testing2"); // this time it should exception out
			fail();
		} catch (Throwable e) {
		}
	}

	@Test
	public void test_getCollection_existing_correct_type() throws Exception {
		db.getCollection(TestValue.class, "testing");
		db.getCollection(TestValue.class, "testing");
	}

	@Test
	public void test_getCollection_invalid_type() {
		insert(10, new TestValue("Bob"));
		try {
			db.getCollection(TestValue2.class, "testing");
			fail();
		} catch(BlueDbException e) {
		}
	}
	
	@Test
	public void test_query_count() throws Exception {
		assertEquals(0, getCollection().query().count());
		BlueKey key = insert(10, new TestValue("Joe", 0));
		assertEquals(1, getCollection().query().count());
		getCollection().delete(key);
		assertEquals(0, getCollection().query().count());
	}

	@Test
	public void test_query_where() throws Exception {
		TestValue valueJoe = new TestValue("Joe");
		TestValue valueBob = new TestValue("Bob");
		insert(1, valueJoe);
		insert(2, valueBob);
		List<TestValue> storedValues;

		storedValues = getCollection().query().getList();
		assertEquals(2, storedValues.size());
		List<TestValue> joeOnly = getCollection().query().where((v) -> v.getName().equals("Joe")).getList();
		assertEquals(1, joeOnly.size());
		assertEquals(valueJoe, joeOnly.get(0));
	}

	@Test
	public void test_query_beforeTime_timeframe() throws Exception {
		TestValue value1to2 = new TestValue("Joe");
		TestValue value2to3 = new TestValue("Bob");
		insert(1, 2, value1to2);
		insert(2, 3, value2to3);

		List<TestValue> before3 = getCollection().query().beforeTime(3).getList();
		List<TestValue> before2 = getCollection().query().beforeTime(2).getList();
		List<TestValue> before1 = getCollection().query().beforeTime(1).getList();
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
		insert(1, valueAt1);
		insert(2, valueAt2);

		List<TestValue> before3 = getCollection().query().beforeTime(3).getList();
		List<TestValue> before2 = getCollection().query().beforeTime(2).getList();
		List<TestValue> before1 = getCollection().query().beforeTime(1).getList();
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
		insert(1, 2, value1to2);
		insert(2, 3, value2to3);

		List<TestValue> beforeOrAt3 = getCollection().query().beforeOrAtTime(3).getList();
		List<TestValue> beforeOrAt2 = getCollection().query().beforeOrAtTime(2).getList();
		List<TestValue> beforeOrAt1 = getCollection().query().beforeOrAtTime(1).getList();
		List<TestValue> beforeOrAt0 = getCollection().query().beforeOrAtTime(0).getList();
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
		insert(1, valueAt1);
		insert(2, valueAt2);

		List<TestValue> beforeOrAt3 = getCollection().query().beforeOrAtTime(3).getList();
		List<TestValue> beforeOrAt2 = getCollection().query().beforeOrAtTime(2).getList();
		List<TestValue> beforeOrAt1 = getCollection().query().beforeOrAtTime(1).getList();
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
		insert(1, 2, value1to2);
		insert(2, 3, value2to3);

		List<TestValue> after3 = getCollection().query().afterTime(3).getList();
		List<TestValue> after2 = getCollection().query().afterTime(2).getList();
		List<TestValue> after1 = getCollection().query().afterTime(1).getList();
		List<TestValue> after0 = getCollection().query().afterTime(0).getList();
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
		insert(1, valueAt1);
		insert(2, valueAt2);

		List<TestValue> after2 = getCollection().query().afterTime(2).getList();
		List<TestValue> after1 = getCollection().query().afterTime(1).getList();
		List<TestValue> after0 = getCollection().query().afterTime(0).getList();
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
		insert(1, 2, value1to2);
		insert(2, 3, value2to3);

		List<TestValue> afterOrAt4 = getCollection().query().afterOrAtTime(4).getList();
		List<TestValue> afterOrAt3 = getCollection().query().afterOrAtTime(3).getList();
		List<TestValue> afterOrAt2 = getCollection().query().afterOrAtTime(2).getList();
		List<TestValue> afterOrAt1 = getCollection().query().afterOrAtTime(1).getList();
		List<TestValue> afterOrAt0 = getCollection().query().afterOrAtTime(0).getList();
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
		insert(1, valueAt1);
		insert(2, valueAt2);

		List<TestValue> afterOrAt2 = getCollection().query().afterOrAtTime(2).getList();
		List<TestValue> afterOrAt1 = getCollection().query().afterOrAtTime(1).getList();
		List<TestValue> afterOrAt0 = getCollection().query().afterOrAtTime(0).getList();
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
		insert(2, valueAt2);
		insert(3, valueAt3);

		// various queries outside the range
		List<TestValue> after0before1 = getCollection().query().afterTime(0).beforeTime(1).getList();
		List<TestValue> after0beforeOrAt1 = getCollection().query().afterTime(0).beforeOrAtTime(1).getList();
		List<TestValue> afterOrAt0before1 = getCollection().query().afterOrAtTime(0).beforeTime(1).getList();
		List<TestValue> afterOrAt0beforeOrAt1 = getCollection().query().afterOrAtTime(0).beforeOrAtTime(1).getList();
		assertEquals(0, after0before1.size());
		assertEquals(0, after0beforeOrAt1.size());
		assertEquals(0, afterOrAt0before1.size());
		assertEquals(0, afterOrAt0beforeOrAt1.size());

		// various queries inside the range
		List<TestValue> after2before3 = getCollection().query().afterTime(2).beforeTime(3).getList();
		List<TestValue> after2beforeOrAt3 = getCollection().query().afterTime(2).beforeOrAtTime(3).getList();
		List<TestValue> afterOrAt2before3 = getCollection().query().afterOrAtTime(2).beforeTime(3).getList();
		List<TestValue> afterOrAt2beforeOrAt3 = getCollection().query().afterOrAtTime(2).beforeOrAtTime(3).getList();
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
		insert(1, valueJoe);
		insert(2, valueBob);
		List<TestValue> storedValues;

		storedValues = getCollection().query().getList();
		assertEquals(2, storedValues.size());
		List<TestValue> joeOnly = getCollection().query().where((v) -> v.getName().equals("Joe")).getList();
		assertEquals(1, joeOnly.size());
		assertEquals(valueJoe, joeOnly.get(0));
	}
	
	@Test
	public void test_getIterator() throws Exception {
		TestValue valueJoe = new TestValue("Joe");
		TestValue valueBob = new TestValue("Bob");
		insert(1, valueJoe);
		insert(2, valueBob);
		Iterator<TestValue> iter;

		iter = getCollection().query().getIterator();
		List<TestValue> list = new ArrayList<>();
		iter.forEachRemaining(list::add);
		assertEquals(2, list.size());
		assertTrue(list.contains(valueJoe));
		assertTrue(list.contains(valueBob));
	}
	
	@Test
	public void test_query_update() throws Exception {
		BlueKey keyJoe   = insert(1, new TestValue("Joe", 0));
		BlueKey keyBob   = insert(2, new TestValue("Bob", 0));
		BlueKey keyJosey = insert(2,  new TestValue("Josey", 0));
		BlueKey keyBobby = insert(3, new TestValue("Bobby", 0));
		BlueQuery<TestValue> queryForJosey = getCollection().query().afterTime(1).where((v) -> v.getName().startsWith("Jo"));

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
		getCollection().query().update((v) -> v.addCupcake());
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
		insert(1, valueJoe);
		insert(2, valueBob);
		insert(2, valueJosey);
		insert(3, valueBobby);
		List<TestValue> storedValues;
		BlueQuery<TestValue> queryForJosey = getCollection().query().afterTime(1).where((v) -> v.getName().startsWith("Jo"));

		// sanity check
		storedValues = getCollection().query().getList();
		assertEquals(4, storedValues.size());
		assertTrue(storedValues.contains(valueJosey));

		// test if delete works with query conditions
		queryForJosey.delete();
		storedValues = getCollection().query().getList();
		assertEquals(3, storedValues.size());
		assertFalse(storedValues.contains(valueJosey));
		assertTrue(storedValues.contains(valueJoe));

		// test if delete works without conditions
		getCollection().query().delete();
		storedValues = getCollection().query().getList();
		assertEquals(0, storedValues.size());
	}

	@Test
	public void test_getAllCollectionsFromDisk() throws Exception {
		getCollection();
		List<BlueCollectionOnDisk<?>> allCollections = db().getAllCollectionsFromDisk();
		assertEquals(1, allCollections.size());
		db().getCollection(String.class, "string");
		db().getCollection(Long.class, "long");
		allCollections = db().getAllCollectionsFromDisk();
		assertEquals(3, allCollections.size());
	}

	@Test
	public void test_backup() throws Exception {
		BlueKey key1At1 = createKey(1, 1);
		TestValue value1 = createValue("Anna");
		getCollection().insert(key1At1, value1);

		BlueCollectionOnDisk<TestValue2> secondCollection = (BlueCollectionOnDisk<TestValue2>) db().getCollection(TestValue2.class, "testing_2");
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
		BlueCollectionOnDisk<TestValue> restoredCollection = (BlueCollectionOnDisk<TestValue>) restoredDb.getCollection(TestValue.class, "testing");
		assertTrue(restoredCollection.contains(key1At1));
		assertEquals(value1, restoredCollection.get(key1At1));
		Long restoredMaxLong = restoredCollection.getMaxLongId();
		assertNotNull(restoredMaxLong);
		assertEquals(getCollection().getMaxLongId().longValue(), restoredMaxLong.longValue());	

		BlueCollectionOnDisk<TestValue2> secondCollectionRestored = (BlueCollectionOnDisk<TestValue2>) restoredDb.getCollection(TestValue2.class, "testing_2");
		assertTrue(secondCollectionRestored.contains(key1At1));
		assertEquals(valueInSecondCollection, secondCollectionRestored.get(key1At1));
	}

	@Test
	public void test_backup_fail() throws Exception {
		@SuppressWarnings("rawtypes")
		BlueCollectionOnDisk newUntypedCollection = db.getCollection("testing2"); // create a new bogus collection
		Path serializedClassesPath = Paths.get(newUntypedCollection.getPath().toString(), ".meta", "serialized_classes");
		getFileManager().saveObject(serializedClassesPath, "some_nonsense");  // serialize a string where there should be a list
		
		Path tempFolder = createTempFolder().toPath();
		tempFolder.toFile().deleteOnExit();
		Path backedUpPath = Paths.get(tempFolder.toString(), "backup_test.zip");
		try {
			db().backup(backedUpPath);
			fail();  // because the "test2" collection was broken, the backup should error out;
		} catch (BlueDbException e) {
		}
	}
}
