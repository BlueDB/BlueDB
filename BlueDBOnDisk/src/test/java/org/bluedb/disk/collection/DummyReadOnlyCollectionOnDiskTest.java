package org.bluedb.disk.collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.UUID;

import org.bluedb.api.CloseableIterator;
import org.bluedb.api.ReadBlueTimeQuery;
import org.bluedb.api.ReadableBlueCollection;
import org.bluedb.api.ReadableBlueTimeCollection;
import org.bluedb.api.datastructures.BlueSimpleInMemorySet;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.TestValue;
import org.junit.Test;

public class DummyReadOnlyCollectionOnDiskTest {

	@Test
	public void test_get() throws BlueDbException {
		ReadableBlueCollection<TestValue> dummyCollection = new DummyReadOnlyCollectionOnDisk<>(TestValue.class);
		assertNull(dummyCollection.get(new IntegerKey(1)));
		assertNull(dummyCollection.getLastKey());
		assertFalse(dummyCollection.contains(new IntegerKey(1)));
	}


	@Test
	public void test_index() throws BlueDbException {
		ReadableBlueCollection<TestValue> dummyCollection = new DummyReadOnlyCollectionOnDisk<>(TestValue.class);
		BlueIndex<ValueKey, TestValue> dummyIndex = dummyCollection.getIndex("nonexisting index", ValueKey.class);
		assertEquals(0, dummyCollection.query()
							.where(dummyIndex.createIntegerIndexCondition()
								.isEqualTo(1))
							.getList()
							.size());
		assertEquals(0, dummyCollection.query()
				.where(dummyIndex.createLongIndexCondition()
					.isEqualTo(1L))
				.getList()
				.size());
		assertEquals(0, dummyCollection.query()
				.where(dummyIndex.createStringIndexCondition()
					.isEqualTo("something"))
				.getList()
				.size());
		assertEquals(0, dummyCollection.query()
				.where(dummyIndex.createUUIDIndexCondition()
					.isEqualTo(UUID.randomUUID()))
				.getList()
				.size());
		assertNull(dummyIndex.getLastKey());
	}


	@Test
	public void test_query() throws BlueDbException, IOException {
		ReadableBlueTimeCollection<TestValue> dummyCollection = new DummyReadOnlyCollectionOnDisk<>(TestValue.class);
		
		ReadBlueTimeQuery<TestValue> dummyQuery = dummyCollection.query()
				.afterOrAtTime(1)
				.beforeOrAtTime(2)
				.where((t) -> true);
		assertEquals(Arrays.asList(), dummyQuery.getList());
		assertEquals(0, dummyQuery.count());
		
		
		CloseableIterator<TestValue> dummyIterator = dummyQuery.getIterator();
		assertFalse(dummyIterator.hasNext());
		assertEquals(null, dummyIterator.next());
		assertEquals(null, dummyIterator.peek());
		dummyIterator.keepAlive();
		dummyIterator.close();
		
		
		dummyQuery = dummyCollection.query()
				.afterOrAtTime(1)
				.beforeOrAtTime(2)
				.whereKeyIsIn(new HashSet<>());
		assertEquals(Arrays.asList(), dummyQuery.getList());
		assertEquals(0, dummyQuery.count());
		
		
		dummyQuery = dummyCollection.query()
				.afterOrAtTime(1)
				.beforeOrAtTime(2)
				.whereKeyIsIn(new BlueSimpleInMemorySet<>(new HashSet<>()));
		assertEquals(Arrays.asList(), dummyQuery.getList());
		assertEquals(0, dummyQuery.count());
	}
}
