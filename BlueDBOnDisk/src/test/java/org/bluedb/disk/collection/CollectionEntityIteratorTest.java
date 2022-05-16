package org.bluedb.disk.collection;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.bluedb.api.BlueCollectionVersion;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.StreamUtils;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.index.conditions.IncludedSegmentRangeInfo;
import org.bluedb.disk.collection.index.conditions.OnDiskIndexCondition;
import org.bluedb.disk.query.QueryIndexConditionGroup;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadWriteSegment;
import org.bluedb.disk.segment.SegmentEntityIterator;
import org.bluedb.disk.serialization.BlueEntity;
import org.junit.Test;
import org.mockito.Mockito;

public class CollectionEntityIteratorTest extends BlueDbDiskTestBase {

	@Test
	public void test_close() throws Exception {
        BlueKey key = createKey(1, 1);
        TestValue value = createValue("Anna");
		ReadWriteSegment<TestValue> segment = getTimeCollection().getSegmentManager().getFirstSegment(key);
        Range range = new Range(1, 1);
        Path chunkPath = Paths.get(segment.getPath().toString(), range.toUnderscoreDelimitedString());

        getTimeCollection().insert(key, value);
        CollectionEntityIterator<TestValue> iterator = new CollectionEntityIterator<>(getTimeSegmentManager(), new Range(1, 2), false, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), Optional.empty());
        assertFalse(getLockManager().isLocked(chunkPath));
        iterator.hasNext();  // force it to open the next file
        assertTrue(getLockManager().isLocked(chunkPath));
        iterator.close();
        assertFalse(getLockManager().isLocked(chunkPath));
        
		try {
			iterator.hasNext();
			fail();
		} catch(RuntimeException e) {
			//Should be thrown
		}
        
		try {
			iterator.peek();
			fail();
		} catch(RuntimeException e) {
			//Should be thrown
		}
        
		try {
			iterator.next();
			fail();
		} catch(RuntimeException e) {
			//Should be thrown
		}
	}

	@Test
	public void test_hasNext() throws Exception {
        BlueKey key1 = createKey(1, 1);
        BlueKey key2 = createKey(2, 2);
        TestValue value1 = createValue("Anna");
        TestValue value2 = createValue("Bob");

        getTimeCollection().insert(key1, value1);
        getTimeCollection().insert(key2, value2);
        CollectionEntityIterator<TestValue> iterator = new CollectionEntityIterator<>(getTimeSegmentManager(), new Range(0, 0), false, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), Optional.empty());
        assertFalse(iterator.hasNext());
        iterator.close();

        iterator = new CollectionEntityIterator<>(getTimeSegmentManager(), new Range(1, 1), false, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), Optional.empty());
        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasNext()); // make sure doing it twice doesn't break anything
        iterator.next();
        assertFalse(iterator.hasNext());
        iterator.close();

        iterator = new CollectionEntityIterator<>(getTimeSegmentManager(), new Range(1, 2), false, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), Optional.empty());
        assertTrue(iterator.hasNext());
        assertEquals(value1, iterator.next().getValue());
        assertTrue(iterator.hasNext());
        assertEquals(value2, iterator.next().getValue());
        assertFalse(iterator.hasNext());
        iterator.close();
	}

	@Test
	public void test_peek() throws Exception {
        BlueKey key1 = createKey(1, 1);
        BlueKey key2 = createKey(2, 2);
        TestValue value1 = createValue("Anna");
        TestValue value2 = createValue("Bob");

        getTimeCollection().insert(key1, value1);
        getTimeCollection().insert(key2, value2);
        CollectionEntityIterator<TestValue> iterator = new CollectionEntityIterator<>(getTimeSegmentManager(), new Range(0, 0), false, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), Optional.empty());
    	assertNull(iterator.peek());
    	iterator.keepAlive();
        iterator.close();

        iterator = new CollectionEntityIterator<>(getTimeSegmentManager(), new Range(1, 1), false, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), Optional.empty());
        assertEquals(value1, iterator.peek().getValue());
        assertEquals(value1, iterator.peek().getValue()); // make sure doing it twice doesn't break anything
        assertEquals(value1, iterator.next().getValue());
    	assertNull(iterator.peek());
        iterator.close();

        iterator = new CollectionEntityIterator<>(getTimeSegmentManager(), new Range(1, 2), false, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), Optional.empty());
        assertEquals(value1, iterator.peek().getValue());
        assertEquals(value1, iterator.next().getValue());
        assertEquals(value2, iterator.peek().getValue());
        assertEquals(value2, iterator.next().getValue());
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

        CollectionEntityIterator<TestValue> iterator = new CollectionEntityIterator<>(getTimeSegmentManager(), new Range(0, 0), false, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), Optional.empty());
        List<BlueEntity<TestValue>> iteratorContents = toList(iterator);
        iterator.close();
        assertEquals(0, iteratorContents.size());

        iterator = new CollectionEntityIterator<>(getTimeSegmentManager(), new Range(0, 1), false, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), Optional.empty());
        iteratorContents = toList(iterator);
        iterator.close();
        assertEquals(1, iteratorContents.size());


        iterator = new CollectionEntityIterator<>(getTimeSegmentManager(), new Range(0, 2), false, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), Optional.empty());
        iteratorContents = new ArrayList<>();
        iteratorContents.add(iterator.next());
        iteratorContents.add(iterator.next());  // make sure next work right after a next
        iterator.close();
        assertEquals(2, iteratorContents.size());
	}

	@Test
	public void test_getNext_multiple_time_frames() {
		long segmentSize = getTimeCollection().getSegmentManager().getSegmentSize();
		ReadWriteSegment<TestValue> firstSegment = getSegment(0);
		ReadWriteSegment<TestValue> secondSegment = getSegment(segmentSize);
		
		TestValue valueInFirstSegment = new TestValue("first");
		TestValue valueInBothSegments = new TestValue("both");
		TestValue valueInSecondSegment = new TestValue("second");
		TestValue valueAfterSecondSegment = new TestValue("after");
		insertAtTimeFrame(0, 1, valueInFirstSegment);
		insertAtTimeFrame(1, segmentSize, valueInBothSegments);
		insertAtTimeFrame(segmentSize + 1, segmentSize + 1, valueInSecondSegment);
		insertAtTimeFrame(segmentSize * 2, segmentSize * 2 + 1, valueAfterSecondSegment);
		List<TestValue> valuesExpectedInFirstSegment = Arrays.asList(valueInFirstSegment, valueInBothSegments);
		List<TestValue> valuesExpectedInSecondSegment = new LinkedList<>(Arrays.asList(valueInSecondSegment));
		if(BlueCollectionVersion.getDefault() == BlueCollectionVersion.VERSION_1) {
			valuesExpectedInSecondSegment.add(0, valueInBothSegments); //Version 1 will include a duplicate record if it overlaps with the segment
		}
		List<TestValue> valuesExpectedInEitherSegment = Arrays.asList(valueInFirstSegment, valueInBothSegments, valueInSecondSegment);

		SegmentEntityIterator<TestValue> firstSegmentIterator = firstSegment.getIterator(0, segmentSize - 1);
		List<TestValue> valuesFromFirstSegment = toValueList(firstSegmentIterator);
		SegmentEntityIterator<TestValue> secondSegmentIterator = secondSegment.getIterator(0, segmentSize * 2 - 1);
		List<TestValue> valuesFromSecondSegment = toValueList(secondSegmentIterator);
		CollectionEntityIterator<TestValue> collectionIterator = new CollectionEntityIterator<>(getTimeSegmentManager(), new Range(0, segmentSize * 2 - 1), false, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), Optional.empty());
		List<TestValue> valuesFromEitherSegment = toValueList(collectionIterator);

		assertEquals(valuesExpectedInFirstSegment, valuesFromFirstSegment);
		assertEquals(valuesExpectedInSecondSegment, valuesFromSecondSegment);
		assertEquals(valuesExpectedInEitherSegment, valuesFromEitherSegment);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void test_getNext_multipleTimeFramesWithIndexConditions() {
		long segmentSize = getTimeCollection().getSegmentManager().getSegmentSize();
		ReadWriteSegment<TestValue> firstSegment = getSegment(0);
		ReadWriteSegment<TestValue> thirdSegment = getSegment(segmentSize * 2);
		ReadWriteSegment<TestValue> fourthSegment = getSegment(segmentSize * 3);
		
		Set<Range> firstThirdAndFourthRanges = new HashSet<>(Arrays.asList(firstSegment.getRange(), thirdSegment.getRange(), fourthSegment.getRange()));
		Set<Range> firstAndFourthRanges = new HashSet<>(Arrays.asList(firstSegment.getRange(), fourthSegment.getRange()));
		Set<Range> fourthRangeOnly = new HashSet<>(Arrays.asList(fourthSegment.getRange()));
		Set<Range> firstAndThirdRanges = new HashSet<>(Arrays.asList(firstSegment.getRange(), thirdSegment.getRange()));
		Set<Range> thirdAndFourthRanges = new HashSet<>(Arrays.asList(thirdSegment.getRange(), fourthSegment.getRange()));
		
		TestValue valueInFirstSegment = new TestValue("first");
		TestValue valueInFirstAndSecondSegments = new TestValue("firstAndSecond");
		TestValue valueInSecondSegment = new TestValue("second");
		TestValue valueInThirdSegment = new TestValue("third");
		TestValue valueInFourthSegment = new TestValue("fourth");
		
		insertAtTimeFrame(0, 1, valueInFirstSegment);
		insertAtTimeFrame(1, segmentSize, valueInFirstAndSecondSegments);
		insertAtTimeFrame(segmentSize + 1, segmentSize + 1, valueInSecondSegment);
		insertAtTimeFrame(segmentSize * 2, segmentSize * 2 + 1, valueInThirdSegment);
		insertAtTimeFrame(segmentSize * 3, segmentSize * 3 + 1, valueInFourthSegment);
		
		long maxTimeToAllow = segmentSize * 3 + 1;
		TestValue ignoredValue = new TestValue("ignored)");
		insertAtTimeFrame(maxTimeToAllow + 1, maxTimeToAllow + 2, ignoredValue);
		
		IncludedSegmentRangeInfo firstThirdAndFourthRangesIncludedSegmentInfo = createFullIncludedSegmentInfoForRanges(firstThirdAndFourthRanges, maxTimeToAllow);
		IncludedSegmentRangeInfo firstAndFourthRangesIncludedSegmentInfo = createFullIncludedSegmentInfoForRanges(firstAndFourthRanges, maxTimeToAllow);
		IncludedSegmentRangeInfo fourthRangeOnlyIncludedSegmentInfo = createFullIncludedSegmentInfoForRanges(fourthRangeOnly, maxTimeToAllow);
		IncludedSegmentRangeInfo firstAndThirdRangesIncludedSegmentInfo = createFullIncludedSegmentInfoForRanges(firstAndThirdRanges, maxTimeToAllow);
		IncludedSegmentRangeInfo thirdAndFourthRangesIncludedSegmentInfo = createFullIncludedSegmentInfoForRanges(thirdAndFourthRanges, maxTimeToAllow);
		
		List<TestValue> valuesExpectedInFourthSegment = Arrays.asList(valueInFourthSegment);
		List<TestValue> valuesExpectedInFirstAndThirdSegment = Arrays.asList(valueInFirstSegment, valueInFirstAndSecondSegments, valueInThirdSegment);
		List<TestValue> valuesExpectedInThirdAndFourthSegment = Arrays.asList(valueInThirdSegment, valueInFourthSegment);
		List<TestValue> valuesExpectedInThirdSegment = Arrays.asList(valueInThirdSegment);
		List<TestValue> valuesExpectedInFirstThirdAndFourthSegment = Arrays.asList(valueInFirstSegment, valueInFirstAndSecondSegments, valueInThirdSegment, valueInFourthSegment);

		OnDiskIndexCondition<?, TestValue> indexConditionSpecifyingNullRanges = Mockito.mock(OnDiskIndexCondition.class);
		Mockito.doReturn(null).when(indexConditionSpecifyingNullRanges).getSegmentRangeInfoToIncludeInCollectionQuery();
		Mockito.doReturn(true).when(indexConditionSpecifyingNullRanges).test(Mockito.any());
		
		OnDiskIndexCondition<?, TestValue> indexConditionSpecifyingFirstThirdAndFourthRanges = Mockito.mock(OnDiskIndexCondition.class);
		Mockito.doReturn(firstThirdAndFourthRangesIncludedSegmentInfo).when(indexConditionSpecifyingFirstThirdAndFourthRanges).getSegmentRangeInfoToIncludeInCollectionQuery();
		Mockito.doReturn(true).when(indexConditionSpecifyingFirstThirdAndFourthRanges).test(Mockito.any());
		
		OnDiskIndexCondition<?, TestValue> indexConditionSpecifyingFirstAndFourthRanges = Mockito.mock(OnDiskIndexCondition.class);
		Mockito.doReturn(firstAndFourthRangesIncludedSegmentInfo).when(indexConditionSpecifyingFirstAndFourthRanges).getSegmentRangeInfoToIncludeInCollectionQuery();
		Mockito.doReturn(true).when(indexConditionSpecifyingFirstAndFourthRanges).test(Mockito.any());
		
		OnDiskIndexCondition<?, TestValue> indexConditionSpecifyingFirstAndThirdRanges = Mockito.mock(OnDiskIndexCondition.class);
		Mockito.doReturn(firstAndThirdRangesIncludedSegmentInfo).when(indexConditionSpecifyingFirstAndThirdRanges).getSegmentRangeInfoToIncludeInCollectionQuery();
		Mockito.doReturn(true).when(indexConditionSpecifyingFirstAndThirdRanges).test(Mockito.any());
		
		OnDiskIndexCondition<?, TestValue> indexConditionSpecifyingThirdAndFourthRanges = Mockito.mock(OnDiskIndexCondition.class);
		Mockito.doReturn(thirdAndFourthRangesIncludedSegmentInfo).when(indexConditionSpecifyingThirdAndFourthRanges).getSegmentRangeInfoToIncludeInCollectionQuery();
		Mockito.doReturn(true).when(indexConditionSpecifyingThirdAndFourthRanges).test(Mockito.any());
		
		//Verify ranges to include works and isn't thrown off if the index condition returns null for its ranges to include
		List<QueryIndexConditionGroup<TestValue>> indexConditionGroups = Arrays.asList(
				new QueryIndexConditionGroup<>(true, Arrays.asList(
						indexConditionSpecifyingNullRanges)));
		Optional<IncludedSegmentRangeInfo> includedSegmentRangeInfo = Optional.of(fourthRangeOnlyIncludedSegmentInfo);
		CollectionEntityIterator<TestValue> collectionIterator = new CollectionEntityIterator<>(getTimeSegmentManager(), Range.createMaxRange(), false, indexConditionGroups, new ArrayList<>(), new ArrayList<>(), includedSegmentRangeInfo);
		List<TestValue> results = toValueList(collectionIterator);
		assertEquals(valuesExpectedInFourthSegment, results);
		
		//Verify first and third index works on its own
		indexConditionGroups = Arrays.asList(new QueryIndexConditionGroup<>(true, Arrays.asList(indexConditionSpecifyingFirstAndThirdRanges)));
		includedSegmentRangeInfo = Optional.empty();
		collectionIterator = new CollectionEntityIterator<>(getTimeSegmentManager(), Range.createMaxRange(), false, indexConditionGroups, new ArrayList<>(), new ArrayList<>(), includedSegmentRangeInfo);
		results = toValueList(collectionIterator);
		assertEquals(valuesExpectedInFirstAndThirdSegment, results);
		
		//Verify third and fourth index works on its own
		indexConditionGroups = Arrays.asList(
				new QueryIndexConditionGroup<>(true, Arrays.asList(
						indexConditionSpecifyingThirdAndFourthRanges)));
		includedSegmentRangeInfo = Optional.empty();
		collectionIterator = new CollectionEntityIterator<>(getTimeSegmentManager(), Range.createMaxRange(), false, indexConditionGroups, new ArrayList<>(), new ArrayList<>(), includedSegmentRangeInfo);
		results = toValueList(collectionIterator);
		assertEquals(valuesExpectedInThirdAndFourthSegment, results);
		
		//Verify that using the "first and third" condition AND the "third and fourth" condition works
		indexConditionGroups = Arrays.asList(
				new QueryIndexConditionGroup<>(true, Arrays.asList(
						indexConditionSpecifyingFirstAndThirdRanges, 
						indexConditionSpecifyingThirdAndFourthRanges)));
		includedSegmentRangeInfo = Optional.empty();
		collectionIterator = new CollectionEntityIterator<>(getTimeSegmentManager(), Range.createMaxRange(), false, indexConditionGroups, new ArrayList<>(), new ArrayList<>(), includedSegmentRangeInfo);
		results = toValueList(collectionIterator);
		assertEquals(valuesExpectedInThirdSegment, results);
		
		//Verify that using the "first and third" condition AND the "third and fourth" condition works (as separate groups)
		indexConditionGroups = Arrays.asList(
				new QueryIndexConditionGroup<>(true, Arrays.asList(
						indexConditionSpecifyingFirstAndThirdRanges)), 
				new QueryIndexConditionGroup<>(true, Arrays.asList(
						indexConditionSpecifyingThirdAndFourthRanges)));
		includedSegmentRangeInfo = Optional.empty();
		collectionIterator = new CollectionEntityIterator<>(getTimeSegmentManager(), Range.createMaxRange(), false, indexConditionGroups, new ArrayList<>(), new ArrayList<>(), includedSegmentRangeInfo);
		results = toValueList(collectionIterator);
		assertEquals(valuesExpectedInThirdSegment, results);
		
		//Verify that using the "first and third" condition OR the "third and fourth" condition works
		indexConditionGroups = Arrays.asList(
				new QueryIndexConditionGroup<>(false, Arrays.asList(
						indexConditionSpecifyingFirstAndThirdRanges, 
						indexConditionSpecifyingThirdAndFourthRanges)));
		includedSegmentRangeInfo = Optional.empty();
		collectionIterator = new CollectionEntityIterator<>(getTimeSegmentManager(), Range.createMaxRange(), false, indexConditionGroups, new ArrayList<>(), new ArrayList<>(), includedSegmentRangeInfo);
		results = toValueList(collectionIterator);
		assertEquals(valuesExpectedInFirstThirdAndFourthSegment, results);
		
		//Verify that using the "first and third" condition OR the "third and fourth" anded with a "first and third" condition works
		indexConditionGroups = Arrays.asList(
				new QueryIndexConditionGroup<>(false, Arrays.asList(
						indexConditionSpecifyingFirstAndThirdRanges, 
						indexConditionSpecifyingThirdAndFourthRanges)), 
				new QueryIndexConditionGroup<>(true, Arrays.asList(indexConditionSpecifyingFirstAndThirdRanges)));
		includedSegmentRangeInfo = Optional.empty();
		collectionIterator = new CollectionEntityIterator<>(getTimeSegmentManager(), Range.createMaxRange(), false, indexConditionGroups, new ArrayList<>(), new ArrayList<>(), includedSegmentRangeInfo);
		results = toValueList(collectionIterator);
		assertEquals(valuesExpectedInFirstAndThirdSegment, results);
		
		//Verify that rangesToInclude takes affect properly
		indexConditionGroups = Arrays.asList(
				new QueryIndexConditionGroup<>(false, Arrays.asList(
						indexConditionSpecifyingFirstAndThirdRanges, 
						indexConditionSpecifyingThirdAndFourthRanges)));
		includedSegmentRangeInfo = Optional.of(firstAndThirdRangesIncludedSegmentInfo);
		collectionIterator = new CollectionEntityIterator<>(getTimeSegmentManager(), Range.createMaxRange(), false, indexConditionGroups, new ArrayList<>(), new ArrayList<>(), includedSegmentRangeInfo);
		results = toValueList(collectionIterator);
		assertEquals(valuesExpectedInFirstAndThirdSegment, results); //Fourth range gets excluded due to rangesToInclude value
		
		//Verify that if an index condition return false in the test method that it will keep anything from being returned
		Mockito.doReturn(false).when(indexConditionSpecifyingFirstThirdAndFourthRanges).test(Mockito.any());
		indexConditionGroups = Arrays.asList(
				new QueryIndexConditionGroup<>(true, Arrays.asList(
						indexConditionSpecifyingFirstThirdAndFourthRanges)));
		includedSegmentRangeInfo = Optional.empty();
		collectionIterator = new CollectionEntityIterator<>(getTimeSegmentManager(), Range.createMaxRange(), false, indexConditionGroups, new ArrayList<>(), new ArrayList<>(), includedSegmentRangeInfo);
		results = toValueList(collectionIterator);
		assertEquals(Arrays.asList(), results);
	}

	private IncludedSegmentRangeInfo createFullIncludedSegmentInfoForRanges(Set<Range> ranges, long maxTimeToAllow) {
		IncludedSegmentRangeInfo includedSegmentInfo = new IncludedSegmentRangeInfo();
		StreamUtils.stream(ranges)
			.forEach(range -> { 
				long groupingNumberStart = Math.min(range.getStart(), maxTimeToAllow);
				long groupingNumberEnd = Math.min(range.getEnd(), maxTimeToAllow);
				includedSegmentInfo.addIncludedSegmentRangeInfo(range, new Range(groupingNumberStart, groupingNumberEnd));
			});
		return includedSegmentInfo;
	}
}
