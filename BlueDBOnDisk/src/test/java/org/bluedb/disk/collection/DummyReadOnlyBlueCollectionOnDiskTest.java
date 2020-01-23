package org.bluedb.disk.collection;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;

import org.bluedb.api.CloseableIterator;
import org.bluedb.api.ReadBlueTimeQuery;
import org.bluedb.api.ReadableBlueCollection;
import org.bluedb.api.ReadableBlueTimeCollection;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.TestValue;
import org.junit.Test;

public class DummyReadOnlyBlueCollectionOnDiskTest {

	@Test
	public void test_get() throws BlueDbException {
		ReadableBlueCollection<TestValue> dummyCollection = new DummyReadOnlyCollectionOnDisk<TestValue>();
		assertNull(dummyCollection.get(new IntegerKey(1)));
		assertNull(dummyCollection.getLastKey());
		assertFalse(dummyCollection.contains(new IntegerKey(1)));
	}


	@Test
	public void test_index() throws BlueDbException {
		ReadableBlueCollection<TestValue> dummyCollection = new DummyReadOnlyCollectionOnDisk<TestValue>();
		BlueIndex<ValueKey, TestValue> dummyIndex = dummyCollection.getIndex("nonexisting index", ValueKey.class);
		assertNull(dummyIndex.get(new IntegerKey(1)));
		assertNull(dummyIndex.getLastKey());
	}


	@Test
	public void test_query() throws BlueDbException, IOException {
		ReadableBlueTimeCollection<TestValue> dummyCollection = new DummyReadOnlyCollectionOnDisk<TestValue>();
		ReadBlueTimeQuery<TestValue> dummyQuery = dummyCollection.query().afterOrAtTime(1).beforeOrAtTime(2).where((t) -> true);
		assertEquals(Arrays.asList(), dummyQuery.getList());
		assertEquals(0, dummyQuery.count());
		CloseableIterator<TestValue> dummyIterator = dummyQuery.getIterator();
		assertFalse(dummyIterator.hasNext());
		assertEquals(null, dummyIterator.next());
		assertEquals(null, dummyIterator.peek());
		dummyIterator.close();
	}
}
