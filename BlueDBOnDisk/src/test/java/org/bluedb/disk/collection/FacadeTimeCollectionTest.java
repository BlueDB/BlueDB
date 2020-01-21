package org.bluedb.disk.collection;

import java.util.Arrays;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.BlueTimeCollection;
import org.bluedb.api.ReadableBlueTimeCollection;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.BlueDbOnDiskBuilder;
import org.bluedb.disk.ReadableBlueDbOnDisk;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.index.TestRetrievalKeyExtractor;
import org.junit.Test;

public class FacadeTimeCollectionTest extends BlueDbDiskTestBase {

	private static final String COLLECTION_NAME = "test_facade_collection";
	private static final TestValue VALUE = new TestValue("John");
	private static final TimeKey KEY = new TimeFrameKey(VALUE.getName(), 1L, 2L);

	@Test
	public void testFacadeTimeCollection() throws BlueDbException {
		ReadableBlueDbOnDisk readOnlyDb = buildReadOnlyBlueDb();
		ReadableBlueTimeCollection<TestValue> facadeCollection = readOnlyDb.getTimeCollection(COLLECTION_NAME, TestValue.class);
		assertTrue(facadeCollection instanceof FacadeTimeCollection);

		IntegerKey key = new IntegerKey(1);
		assertFalse(facadeCollection.contains(key));
		BlueCollection<TestValue> writeableCollection = db().getCollectionBuilder(COLLECTION_NAME, IntegerKey.class, TestValue.class).build();
		writeableCollection.insert(key, VALUE);
		assertFalse(facadeCollection.contains(key)); // wrong collection type, so still using dummy collection
	}

	@Test
	public void testGetIndex() throws BlueDbException {
		ReadableBlueDbOnDisk readOnlyDb = buildReadOnlyBlueDb();
		ReadableBlueTimeCollection<TestValue> facadeCollection = readOnlyDb.getTimeCollection(COLLECTION_NAME, TestValue.class);
		assertTrue(facadeCollection instanceof FacadeTimeCollection);
		String indexName = "dummy_index";

		assertNull(facadeCollection.getIndex(indexName, IntegerKey.class).getLastKey());
		BlueTimeCollection<TestValue> writeableCollection = buildUnderlyingCollection();
		writeableCollection.createIndex(indexName, IntegerKey.class, new TestRetrievalKeyExtractor());
		writeableCollection.insert(KEY, VALUE);
		assertNotNull(facadeCollection.getIndex(indexName, IntegerKey.class).getLastKey());
	}

	@Test
	public void testContains() throws BlueDbException {
		ReadableBlueDbOnDisk readOnlyDb = buildReadOnlyBlueDb();
		ReadableBlueTimeCollection<TestValue> facadeCollection = readOnlyDb.getTimeCollection(COLLECTION_NAME, TestValue.class);
		assertTrue(facadeCollection instanceof FacadeTimeCollection);

		assertFalse(facadeCollection.contains(KEY));
		BlueTimeCollection<TestValue> writeableCollection = buildUnderlyingCollection();
		writeableCollection.insert(KEY, VALUE);
		assertTrue(facadeCollection.contains(KEY));
	}

	@Test
	public void testGet() throws BlueDbException {
		ReadableBlueDbOnDisk readOnlyDb = buildReadOnlyBlueDb();
		ReadableBlueTimeCollection<TestValue> facadeCollection = readOnlyDb.getTimeCollection(COLLECTION_NAME, TestValue.class);
		assertTrue(facadeCollection instanceof FacadeTimeCollection);

		assertNull(facadeCollection.get(KEY));
		BlueTimeCollection<TestValue> writeableCollection = buildUnderlyingCollection();
		writeableCollection.insert(KEY, VALUE);
		assertEquals(VALUE, facadeCollection.get(KEY));
	}

	@Test
	public void testGetLastKey() throws BlueDbException {
		ReadableBlueDbOnDisk readOnlyDb = buildReadOnlyBlueDb();
		ReadableBlueTimeCollection<TestValue> facadeCollection = readOnlyDb.getTimeCollection(COLLECTION_NAME, TestValue.class);
		assertTrue(facadeCollection instanceof FacadeTimeCollection);

		assertNull(facadeCollection.getLastKey());
		BlueTimeCollection<TestValue> writeableCollection = buildUnderlyingCollection();
		writeableCollection.insert(KEY, VALUE);
		assertEquals(KEY, facadeCollection.getLastKey());
	}

	@Test
	public void testQuery() throws BlueDbException {
		ReadableBlueDbOnDisk readOnlyDb = buildReadOnlyBlueDb();
		ReadableBlueTimeCollection<TestValue> facadeCollection = readOnlyDb.getTimeCollection(COLLECTION_NAME, TestValue.class);
		assertTrue(facadeCollection instanceof FacadeTimeCollection);

		assertEquals(Arrays.asList(), facadeCollection.query().getList());
		BlueTimeCollection<TestValue> writeableCollection = buildUnderlyingCollection();
		writeableCollection.insert(KEY, VALUE);
		assertEquals(Arrays.asList(VALUE), facadeCollection.query().getList());
	}


	private BlueTimeCollection<TestValue> buildUnderlyingCollection() throws BlueDbException {
		return db().getTimeCollectionBuilder(COLLECTION_NAME, TimeFrameKey.class, TestValue.class).build();
	}

	private ReadableBlueDbOnDisk buildReadOnlyBlueDb() throws BlueDbException {
		return (ReadableBlueDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db().getPath()).buildReadOnly();
	}
}
