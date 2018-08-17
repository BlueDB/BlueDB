package io.bluedb.disk.collection;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.segment.Segment;
import io.bluedb.disk.segment.SegmentEntityIterator;
import io.bluedb.disk.segment.Range;
import io.bluedb.disk.serialization.BlueEntity;

public class CollectionEntityIteratorTest extends BlueDbDiskTestBase {

	@Test
	public void test_close() throws Exception {
        BlueKey key = createKey(1, 1);
        TestValue value = createValue("Anna");
		Segment<TestValue> segment = getTimeCollection().getSegmentManager().getFirstSegment(key);
        Range range = new Range(1, 1);
        Path chunkPath = Paths.get(segment.getPath().toString(), range.toUnderscoreDelimitedString());

        getTimeCollection().insert(key, value);
        CollectionEntityIterator<TestValue> iterator = new CollectionEntityIterator<>(getTimeCollection(), new Range(1, 2), false);
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
        CollectionEntityIterator<TestValue> iterator = new CollectionEntityIterator<>(getTimeCollection(), new Range(0, 0), false);
        assertFalse(iterator.hasNext());
        iterator.close();

        iterator = new CollectionEntityIterator<>(getTimeCollection(), new Range(1, 1), false);
        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasNext()); // make sure doing it twice doesn't break anything
        iterator.next();
        assertFalse(iterator.hasNext());
        iterator.close();

        iterator = new CollectionEntityIterator<>(getTimeCollection(), new Range(1, 2), false);
        assertTrue(iterator.hasNext());
        assertEquals(value1, iterator.next().getValue());
        assertTrue(iterator.hasNext());
        assertEquals(value2, iterator.next().getValue());
        assertFalse(iterator.hasNext());
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

        CollectionEntityIterator<TestValue> iterator = new CollectionEntityIterator<>(getTimeCollection(), new Range(0, 0), false);
        List<BlueEntity<TestValue>> iteratorContents = toList(iterator);
        iterator.close();
        assertEquals(0, iteratorContents.size());

        iterator = new CollectionEntityIterator<>(getTimeCollection(), new Range(0, 1), false);
        iteratorContents = toList(iterator);
        iterator.close();
        assertEquals(1, iteratorContents.size());


        iterator = new CollectionEntityIterator<>(getTimeCollection(), new Range(0, 2), false);
        iteratorContents = new ArrayList<>();
        iteratorContents.add(iterator.next());
        iteratorContents.add(iterator.next());  // make sure next work right after a next
        iterator.close();
        assertEquals(2, iteratorContents.size());
	}

	@Test
	public void test_getNext_multiple_time_frames() {
		long segmentSize = getTimeCollection().getSegmentManager().getSegmentSize();
		Segment<TestValue> firstSegment = getSegment(0);
		Segment<TestValue> secondSegment = getSegment(segmentSize);
		
		TestValue valueInFirstSegment = new TestValue("first");
		TestValue valueInBothSegments = new TestValue("both");
		TestValue valueInSecondSegment = new TestValue("second");
		TestValue valueAfterSecondSegment = new TestValue("after");
		insertAtTimeFrame(0, 1, valueInFirstSegment);
		insertAtTimeFrame(1, segmentSize, valueInBothSegments);
		insertAtTimeFrame(segmentSize + 1, segmentSize + 1, valueInSecondSegment);
		insertAtTimeFrame(segmentSize * 2, segmentSize * 2 + 1, valueAfterSecondSegment);
		List<TestValue> valuesExpectedInFirstSegment = Arrays.asList(valueInFirstSegment, valueInBothSegments);
		List<TestValue> valuesExpectedInSecondSegment = Arrays.asList(valueInBothSegments, valueInSecondSegment);
		List<TestValue> valuesExpectedInEitherSegment = Arrays.asList(valueInFirstSegment, valueInBothSegments, valueInSecondSegment);

		SegmentEntityIterator<TestValue> firstSegmentIterator = firstSegment.getIterator(0, segmentSize - 1);
		List<TestValue> valuesFromFirstSegment = toValueList(firstSegmentIterator);
		SegmentEntityIterator<TestValue> secondSegmentIterator = secondSegment.getIterator(0, segmentSize * 2 - 1);
		List<TestValue> valuesFromSecondSegment = toValueList(secondSegmentIterator);
		CollectionEntityIterator<TestValue> collectionIterator = new CollectionEntityIterator<>(getTimeCollection(), new Range(0, segmentSize * 2 - 1), false);
		List<TestValue> valuesFromEitherSegment = toValueList(collectionIterator);

		assertEquals(valuesExpectedInFirstSegment, valuesFromFirstSegment);
		assertEquals(valuesExpectedInSecondSegment, valuesFromSecondSegment);
		assertEquals(valuesExpectedInEitherSegment, valuesFromEitherSegment);
	}
}
