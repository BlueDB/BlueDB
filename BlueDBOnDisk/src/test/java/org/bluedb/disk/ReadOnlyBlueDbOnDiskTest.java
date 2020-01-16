package org.bluedb.disk;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.ReadableBlueCollection;
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
}
