package io.bluedb.disk.segment;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.file.BlueObjectInput;
import io.bluedb.disk.file.BlueObjectOutput;
import io.bluedb.disk.serialization.BlueEntity;

public class SegmentEntityIteratorTest extends BlueDbDiskTestBase {

	@Test
	public void test_close() {
		Segment<TestValue> segment = getSegment(1);
		BlueKey key = createKey(1, 1);
		TestValue value = createValue("Anna");
		try {
			segment.insert(key, value);
			SegmentEntityIterator<TestValue> iterator = segment.getIterator(1, 1);
			assertNull(iterator.getCurrentPath());  // it should not have opened anything until it needs it
			iterator.hasNext();  // force it to open the next file
			assertNotNull(iterator.getCurrentPath());
			assertTrue(getLockManager().isLocked(iterator.getCurrentPath()));
			iterator.close();
			assertFalse(getLockManager().isLocked(iterator.getCurrentPath()));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_different_ranges() {
		Segment<TestValue> segment = getSegment(1);
		BlueKey key1 = createKey(1, 1);
		BlueKey key2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		try {
			segment.insert(key1, value1);
			segment.insert(key2, value2);

			SegmentEntityIterator<TestValue> iterator = segment.getIterator(0, 0);
			List<BlueEntity<TestValue>> entities = toList(iterator);
			iterator.close();
			assertEquals(0, entities.size());

			iterator = segment.getIterator(3, 4);
			entities = toList(iterator);
			iterator.close();
			assertEquals(0, entities.size());

			iterator = segment.getIterator(1, 1);
			entities = toList(iterator);
			iterator.close();
			assertEquals(1, entities.size());

			iterator = segment.getIterator(1, 2);
			entities = toList(iterator);
			iterator.close();
			assertEquals(2, entities.size());

		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_hasNext() {
		Segment<TestValue> segment = getSegment(1);
		BlueKey key1 = createKey(1, 1);
		BlueKey key2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		try {
			segment.insert(key1, value1);
			segment.insert(key2, value2);
			SegmentEntityIterator<TestValue> iterator = segment.getIterator(1, 2);
			assertTrue(iterator.hasNext());
			assertTrue(iterator.hasNext());  // make sure the second call doesn't break anything
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
		Segment<TestValue> segment = getSegment(1);
		BlueKey key1 = createKey(1, 1);
		BlueKey key2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		try {
			segment.insert(key1, value1);
			segment.insert(key2, value2);
			SegmentEntityIterator<TestValue> iterator = segment.getIterator(1, 2);
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
	public void test_next_rollup_before_reads() {
		Segment<TestValue> segment = getSegment(1);
		BlueKey key1 = createKey(1, 1);
		BlueKey key2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		try {
			segment.insert(key1, value1);
			segment.insert(key2, value2);
			SegmentEntityIterator<TestValue> iterator = segment.getIterator(1, 2); // it should now have two ranges to search
			List<BlueEntity<TestValue>> entities = toList(iterator);
			assertEquals(2, entities.size());
			
			iterator = segment.getIterator(1, 2); // it should now have two ranges to search
			long segmentSize = SegmentManager.getSegmentSize();
			Range range = new Range(0, segmentSize -1);
			segment.rollup(range);
			entities = toList(iterator);
			assertEquals(2, entities.size());

			iterator.close();
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_next_rollup_during_reads() {
		Segment<TestValue> segment = getSegment(1);
		BlueKey key1 = createKey(1, 1);
		BlueKey key2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		try {
			segment.insert(key1, value1);
			segment.insert(key2, value2);
			SegmentEntityIterator<TestValue> iterator = segment.getIterator(1, 2); // it should now have two files to read
			List<BlueEntity<TestValue>> entities = new ArrayList<>();
			entities.add(iterator.next()); // read one from the first file;

			// simulate a rollup from underneath us
			long segmentSize = SegmentManager.getSegmentSize();
			Range range = new Range(0, segmentSize -1);
			Path rolledUpPath = Paths.get(segment.getPath().toString(), range.toUnderscoreDelimitedString());
			try (BlueObjectOutput<BlueEntity<TestValue>> output = segment.getObjectOutputFor(rolledUpPath)) {
				output.write(new BlueEntity<TestValue>(key1, value1));
				output.write(new BlueEntity<TestValue>(key2, value2));
			}
			Range rangeToRemove = new Range(2,2);
			Path pathToRemove = Paths.get(segment.getPath().toString(), rangeToRemove.toUnderscoreDelimitedString());
			assertTrue(pathToRemove.toFile().delete());

			// add the remaining items
			while (iterator.hasNext()) {
				BlueEntity<TestValue> next = iterator.next();
				entities.add(next);
			}

			assertEquals(2, entities.size());

			iterator.close();
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_getNextStream_file_deleted() {
		Segment<TestValue> segment = getSegment(1);
		BlueKey key1 = createKey(1, 1);
		BlueKey key2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		try {
			segment.insert(key1, value1);
			segment.insert(key2, value2);
			SegmentEntityIterator<TestValue> iterator = segment.getIterator(1, 2); // it should now have two files to read
			List<BlueEntity<TestValue>> entities = new ArrayList<>();
			entities.add(iterator.next()); // read one from the first file;

			// rip out the second file
			Range rangeToRemove = new Range(2,2);
			Path pathToRemove = Paths.get(segment.getPath().toString(), rangeToRemove.toUnderscoreDelimitedString());
			assertTrue(pathToRemove.toFile().delete());

			// add the remaining items
			while (iterator.hasNext()) {
				BlueEntity<TestValue> next = iterator.next();
				entities.add(next);
			}

			assertEquals(1, entities.size());

			iterator.close();
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_getNextStream_exception() throws Exception {
		Segment<TestValue> segment = getSegment(1);
		BlueKey key1 = createKey(1, 1);
		BlueKey key2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		segment.insert(key1, value1);
		segment.insert(key2, value2);

		
		SegmentEntityIterator<TestValue> iterator = segment.getIterator(1, 2);
		List<BlueEntity<TestValue>> entitiesInRealSegment = new ArrayList<BlueEntity<TestValue>>();
		while(iterator.hasNext()) {
			entitiesInRealSegment.add(iterator.next());
		}
		
		assertEquals(2, entitiesInRealSegment.size());

		Segment<TestValue> mockSegment = new Segment<TestValue>(segment.getPath(), getFileManager()) {
			@Override
			protected BlueObjectInput<BlueEntity<TestValue>> getObjectInputFor(long groupingNumber) throws BlueDbException {
				throw new BlueDbException("segment fail");
			}
		};
		
		SegmentEntityIterator<TestValue> iterator2 = mockSegment.getIterator(1, 2);
		List<BlueEntity<TestValue>> entitiesFromBrokenSegment = new ArrayList<BlueEntity<TestValue>>();
		while(iterator2.hasNext()) {
			entitiesFromBrokenSegment.add(iterator2.next());
		}

		assertEquals(0, entitiesFromBrokenSegment.size());
	}

	@Test
	public void test_filesToRanges() {
		// TODO
	}
}
