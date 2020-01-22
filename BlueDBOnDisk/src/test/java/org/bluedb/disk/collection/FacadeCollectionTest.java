package org.bluedb.disk.collection;

import java.util.Arrays;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.ReadableBlueCollection;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.BlueDbOnDiskBuilder;
import org.bluedb.disk.ReadableBlueDbOnDisk;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.index.FacadeBlueIndexOnDisk;
import org.bluedb.disk.collection.index.TestRetrievalKeyExtractor;
import org.junit.Test;

public class FacadeCollectionTest extends BlueDbDiskTestBase {

	private static final String COLLECTION_NAME = "test_facade_collection";
	private static final TestValue VALUE = new TestValue("John");
	private static final IntegerKey KEY = new IntegerKey(1);

	@Test
	public void testFacadeCollection() throws BlueDbException {
		ReadableBlueDbOnDisk readOnlyDb = buildReadOnlyBlueDb();
		ReadableBlueCollection<TestValue> facadeCollection = readOnlyDb.getCollection(COLLECTION_NAME, TestValue.class);
		assertTrue(facadeCollection instanceof FacadeCollection);

		IntegerKey key = new IntegerKey(1);
		assertFalse(facadeCollection.contains(key));
		BlueCollection<String> writeableCollection = db().getCollectionBuilder(COLLECTION_NAME, IntegerKey.class, String.class).build();
		writeableCollection.insert(key, "whatever");
		readOnlyDb.getCollection(COLLECTION_NAME, String.class);  // make sure the read-only collection is created with the correct type first
		assertFalse(facadeCollection.contains(key)); // wrong collection type, so still using dummy collection
	}

	@Test
	public void testGetIndex() throws BlueDbException {
		ReadableBlueDbOnDisk readOnlyDb = buildReadOnlyBlueDb();
		ReadableBlueCollection<TestValue> facadeCollection = readOnlyDb.getCollection(COLLECTION_NAME, TestValue.class);
		assertTrue(facadeCollection instanceof FacadeCollection);
		String indexName = "dummy_index";
		BlueIndex<IntegerKey, TestValue> facadeIndex = facadeCollection.getIndex(indexName, IntegerKey.class);
		assertTrue(facadeIndex instanceof FacadeBlueIndexOnDisk);

		assertNull(facadeCollection.getIndex(indexName, IntegerKey.class).getLastKey());
		assertNull(facadeIndex.getLastKey());
		BlueCollection<TestValue> writeableCollection = buildUnderlyingCollection();
		writeableCollection.insert(KEY, VALUE);
		assertNull(facadeCollection.getIndex(indexName, IntegerKey.class).getLastKey());  // still null because no real index
		assertNull(facadeIndex.getLastKey());  // still null because no real index
		writeableCollection.createIndex(indexName, IntegerKey.class, new TestRetrievalKeyExtractor());
		writeableCollection.update(KEY, (v) -> v.addCupcake());
		assertNotNull(facadeCollection.getIndex(indexName, IntegerKey.class).getLastKey());
		assertNotNull(facadeIndex.getLastKey());
	}

	@Test
	public void testContains() throws BlueDbException {
		ReadableBlueDbOnDisk readOnlyDb = buildReadOnlyBlueDb();
		ReadableBlueCollection<TestValue> facadeCollection = readOnlyDb.getCollection(COLLECTION_NAME, TestValue.class);
		assertTrue(facadeCollection instanceof FacadeCollection);

		assertFalse(facadeCollection.contains(KEY));
		BlueCollection<TestValue> writeableCollection = buildUnderlyingCollection();
		writeableCollection.insert(KEY, VALUE);
		assertTrue(facadeCollection.contains(KEY));
	}

	@Test
	public void testGet() throws BlueDbException {
		ReadableBlueDbOnDisk readOnlyDb = buildReadOnlyBlueDb();
		ReadableBlueCollection<TestValue> facadeCollection = readOnlyDb.getCollection(COLLECTION_NAME, TestValue.class);
		assertTrue(facadeCollection instanceof FacadeCollection);

		assertNull(facadeCollection.get(KEY));
		BlueCollection<TestValue> writeableCollection = buildUnderlyingCollection();
		writeableCollection.insert(KEY, VALUE);
		assertEquals(VALUE, facadeCollection.get(KEY));
	}

	@Test
	public void testGetLastKey() throws BlueDbException {
		ReadableBlueDbOnDisk readOnlyDb = buildReadOnlyBlueDb();
		ReadableBlueCollection<TestValue> facadeCollection = readOnlyDb.getCollection(COLLECTION_NAME, TestValue.class);
		assertTrue(facadeCollection instanceof FacadeCollection);

		assertNull(facadeCollection.getLastKey());
		BlueCollection<TestValue> writeableCollection = buildUnderlyingCollection();
		writeableCollection.insert(KEY, VALUE);
		assertEquals(KEY, facadeCollection.getLastKey());
	}

	@Test
	public void testQuery() throws BlueDbException {
		ReadableBlueDbOnDisk readOnlyDb = buildReadOnlyBlueDb();
		ReadableBlueCollection<TestValue> facadeCollection = readOnlyDb.getCollection(COLLECTION_NAME, TestValue.class);
		assertTrue(facadeCollection instanceof FacadeCollection);

		assertEquals(Arrays.asList(), facadeCollection.query().getList());
		BlueCollection<TestValue> writeableCollection = buildUnderlyingCollection();
		writeableCollection.insert(KEY, VALUE);
		assertEquals(Arrays.asList(VALUE), facadeCollection.query().getList());
	}


	private BlueCollection<TestValue> buildUnderlyingCollection() throws BlueDbException {
		return db().getCollectionBuilder(COLLECTION_NAME, IntegerKey.class, TestValue.class).build();
	}

	private ReadableBlueDbOnDisk buildReadOnlyBlueDb() throws BlueDbException {
		return (ReadableBlueDbOnDisk) (new BlueDbOnDiskBuilder()).withPath(db().getPath()).buildReadOnly();
	}
}
