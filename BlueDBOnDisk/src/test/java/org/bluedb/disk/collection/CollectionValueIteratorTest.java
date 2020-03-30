package org.bluedb.disk.collection;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.lock.LockManager;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadWriteSegment;
import org.junit.Test;

public class CollectionValueIteratorTest extends BlueDbDiskTestBase {

	@Override
	public void setUp() throws Exception {
		super.setUp();
	}

	@Override
	public void tearDown() throws Exception {
		super.tearDown();
	}

	@Test
	public void test_close() throws Exception {
		BlueKey key = createKey(1, 1);
		TestValue value = createValue("Anna");
		ReadWriteSegment<TestValue> segment = getTimeCollection().getSegmentManager().getFirstSegment(key);
		Range range = new Range(1, 1);
		Path chunkPath = Paths.get(segment.getPath().toString(), range.toUnderscoreDelimitedString());

        getTimeCollection().insert(key, value);
        CollectionValueIterator<TestValue> iterator = (CollectionValueIterator<TestValue>) getTimeCollection().query().afterOrAtTime(1).beforeOrAtTime(2).getIterator();
        assertFalse(getLockManager().isLocked(chunkPath));
        iterator.hasNext();  // force it to open the next file
        assertTrue(getLockManager().isLocked(chunkPath));
        iterator.close();
        assertFalse(getLockManager().isLocked(chunkPath));
	}

	@Test
	public void test_hasNext() throws Exception {
        BlueKey key1 = createKey(1, 1);
        BlueKey key2 = createKey(2, 2);
        TestValue value1 = createValue("Anna");
        TestValue value2 = createValue("Bob");
        getTimeCollection().insert(key1, value1);
        getTimeCollection().insert(key2, value2);
        CollectionValueIterator<TestValue> iterator = (CollectionValueIterator<TestValue>) getTimeCollection().query().afterOrAtTime(0).beforeOrAtTime(0).getIterator();
        assertFalse(iterator.hasNext());
        iterator.close();

        iterator = (CollectionValueIterator<TestValue>) getTimeCollection().query().afterOrAtTime(1).beforeOrAtTime(1).getIterator();
        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasNext()); // make sure doing it twice doesn't break anything
        iterator.next();
        assertFalse(iterator.hasNext());
        iterator.close();

        iterator = (CollectionValueIterator<TestValue>) getTimeCollection().query().afterOrAtTime(1).beforeOrAtTime(2).getIterator();
        assertTrue(iterator.hasNext());
        iterator.next();
        assertTrue(iterator.hasNext());
        iterator.next();
        assertFalse(iterator.hasNext());
        iterator.close();
	}

	@Test
	public void test_peekNext() throws Exception {
        BlueKey key1 = createKey(1, 1);
        BlueKey key2 = createKey(2, 2);
        TestValue value1 = createValue("Anna");
        TestValue value2 = createValue("Bob");
        getTimeCollection().insert(key1, value1);
        getTimeCollection().insert(key2, value2);
        CollectionValueIterator<TestValue> iterator = (CollectionValueIterator<TestValue>) getTimeCollection().query().afterOrAtTime(0).beforeOrAtTime(0).getIterator();
    	assertNull(iterator.peek());
        iterator.close();

        iterator = (CollectionValueIterator<TestValue>) getTimeCollection().query().afterOrAtTime(1).beforeOrAtTime(1).getIterator();
        assertEquals(value1, iterator.peek());
        assertEquals(value1, iterator.peek()); // make sure doing it twice doesn't break anything
        assertEquals(value1, iterator.next());
    	assertNull(iterator.peek());
        iterator.close();

        iterator = (CollectionValueIterator<TestValue>) getTimeCollection().query().afterOrAtTime(1).beforeOrAtTime(2).getIterator();
        assertEquals(value1, iterator.peek());
        assertEquals(value1, iterator.next());
        assertEquals(value2, iterator.peek());
        assertEquals(value2, iterator.next());
    	assertNull(iterator.peek());
        iterator.close();
	}

	@Test
	public void test_next() throws Exception {
        BlueKey key1 = createKey(1, 1);
        BlueKey key2 = createKey(2, 2);
        TestValue value1 = createValue("Anna");
        TestValue value2 = createValue("Bob");

        getTimeCollection().insert(key1, value1);
        getTimeCollection().insert(key2, value2);

        CollectionValueIterator<TestValue> iterator = (CollectionValueIterator<TestValue>) getTimeCollection().query().afterOrAtTime(0).beforeOrAtTime(0).getIterator();
        List<TestValue> iteratorContents = toList(iterator);
        iterator.close();
        assertEquals(0, iteratorContents.size());

        iterator = (CollectionValueIterator<TestValue>) getTimeCollection().query().afterOrAtTime(0).beforeOrAtTime(1).getIterator();
        iteratorContents = toList(iterator);
        iterator.close();
        assertEquals(1, iteratorContents.size());


        iterator = (CollectionValueIterator<TestValue>) getTimeCollection().query().afterOrAtTime(1).beforeOrAtTime(2).getIterator();
        iteratorContents = toList(iterator);
        iterator.close();
        assertEquals(2, iteratorContents.size());
	}


	@Test
	public void test_timeout() throws Exception {
		BlueKey key1at1 = createKey(1, 1);
		BlueKey key2at1 = createKey(2, 1);
		BlueKey key3at3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		TestValue value3 = createValue("Charlie");
		List<TestValue> iteratorContents;
		
		// insert values
        getTimeCollection().insert(key1at1, value1);
        getTimeCollection().insert(key2at1, value2);
        getTimeCollection().insert(key3at3, value3);

		// validate that the iterator is working normally
		try (CollectionValueIterator<TestValue> iterator =  new CollectionValueIterator<>(getTimeSegmentManager(), new Range(0, 3), false, new ArrayList<>())) {
			iteratorContents = toList(iterator);
			iterator.close();
			assertEquals(3, iteratorContents.size());
		}

		LockManager<Path> lockManager = getLockManager();
		Path segmentPath = getTimeCollection().getSegmentManager().getFirstSegment(key2at1).getPath();
		String fileName = Range.forValueAndRangeSize(1, 1).toUnderscoreDelimitedString();
		Path firstFilePath = Paths.get(segmentPath.toString(), fileName);

		try (CollectionValueIterator<TestValue> iterator =  new CollectionValueIterator<>(getTimeSegmentManager(), new Range(0, 3), 20, false, new ArrayList<>())) {
			TestValue first = iterator.next(); // make sure that the underlying resources are acquired
			assertEquals(value1, first);

			assertTrue(lockManager.isLocked(firstFilePath));

			for(int i = 0; i < 10; i++) {
				Blutils.trySleep(6);
				iterator.keepAlive();
				assertTrue(lockManager.isLocked(firstFilePath)); //Keep alive should keep it from closing
			}
			
			Blutils.trySleep(60); // let the iterator auto-close
			assertFalse(lockManager.isLocked(firstFilePath));  // make sure the lock is released
			
			try {
				// make sure the iterator throws an error
				iterator.next(); iterator.next(); iterator.next(); iterator.next();
				fail();
			} catch (Exception e) {}

			try {
				iterator.hasNext();  // make sure the iterator throws an error
				fail();
			} catch (Exception e) {}

			try {
				iterator.peek();  // make sure the iterator throws an error
				fail();
			} catch (Exception e) {}
		}
	}
}