package io.bluedb.disk;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Test;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.collection.CollectionEntityIterator;
import io.bluedb.disk.segment.Segment;
import io.bluedb.disk.segment.TimeRange;
import io.bluedb.disk.serialization.BlueEntity;

public class BlIteratorTest extends BlueDbDiskTestBase {

	@Override
	public void setUp() throws Exception {
		super.setUp();
	}

	@Override
	public void tearDown() throws Exception {
		super.tearDown();
	}

	@Test
	public void test_close() {
		BlueKey key = createKey(1, 1);
		TestValue value = createValue("Anna");
		Segment<TestValue> segment = getCollection().getSegmentManager().getFirstSegment(key);
		TimeRange range = new TimeRange(1, 1);
		Path chunkPath = Paths.get(segment.getPath().toString(), range.toUnderscoreDelimitedString());
		try {
			getCollection().insert(key, value);
			BlIterator<TestValue> iterator = (BlIterator<TestValue>) getCollection().query().afterOrAtTime(1).beforeOrAtTime(2).getIterator();
			assertFalse(getLockManager().isLocked(chunkPath));
			iterator.hasNext();  // force it to open the next file
			assertTrue(getLockManager().isLocked(chunkPath));
			iterator.close();
			assertFalse(getLockManager().isLocked(chunkPath));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_hasNext() {
		BlueKey key1 = createKey(1, 1);
		BlueKey key2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		try {
			getCollection().insert(key1, value1);
			getCollection().insert(key2, value2);
			BlIterator<TestValue> iterator = (BlIterator<TestValue>) getCollection().query().afterOrAtTime(0).beforeOrAtTime(0).getIterator();
			assertFalse(iterator.hasNext());
			iterator.close();

			iterator = (BlIterator<TestValue>) getCollection().query().afterOrAtTime(1).beforeOrAtTime(1).getIterator();
			assertTrue(iterator.hasNext());
			assertTrue(iterator.hasNext()); // make sure doing it twice doesn't break anything
			iterator.next();
			assertFalse(iterator.hasNext());
			iterator.close();

			iterator = (BlIterator<TestValue>) getCollection().query().afterOrAtTime(1).beforeOrAtTime(2).getIterator();
			assertTrue(iterator.hasNext());
			iterator.next();
			assertTrue(iterator.hasNext());
			iterator.next();
			assertFalse(iterator.hasNext());
			iterator.close();
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_next() {
		BlueKey key1 = createKey(1, 1);
		BlueKey key2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		try {
			getCollection().insert(key1, value1);
			getCollection().insert(key2, value2);
			
			BlIterator<TestValue> iterator = (BlIterator<TestValue>) getCollection().query().afterOrAtTime(0).beforeOrAtTime(0).getIterator();
			List<TestValue> iteratorContents = toList(iterator);
			iterator.close();
			assertEquals(0, iteratorContents.size());

			iterator = (BlIterator<TestValue>) getCollection().query().afterOrAtTime(0).beforeOrAtTime(1).getIterator();
			iteratorContents = toList(iterator);
			iterator.close();
			assertEquals(1, iteratorContents.size());


			iterator = (BlIterator<TestValue>) getCollection().query().afterOrAtTime(1).beforeOrAtTime(2).getIterator();
			iteratorContents = toList(iterator);
			iterator.close();
			assertEquals(2, iteratorContents.size());

		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

}