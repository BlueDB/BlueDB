package org.bluedb.disk.collection.index.conditions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.ReadableCollectionOnDisk;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadableSegmentManager;
import org.bluedb.disk.segment.path.SegmentPathManager;
import org.bluedb.disk.serialization.BlueEntity;
import org.junit.Test;
import org.mockito.Mockito;

public class AllSegmentsInRangeAcceptingIndexConditionTest {

	@SuppressWarnings("unchecked")
	@Test
	public void test_unsupportedOperationsThrowException() throws BlueDbException {
		ReadableCollectionOnDisk<TestValue> collectionMock = (ReadableCollectionOnDisk<TestValue>) Mockito.mock(ReadableCollectionOnDisk.class);
		ReadableSegmentManager<TestValue> segmentManagerMock = (ReadableSegmentManager<TestValue>) Mockito.mock(ReadableSegmentManager.class);
		SegmentPathManager segmentPathManagerMock = (SegmentPathManager) Mockito.mock(SegmentPathManager.class);
		
		Mockito.doReturn(segmentPathManagerMock).when(segmentManagerMock).getPathManager();
		Mockito.doReturn(segmentManagerMock).when(collectionMock).getSegmentManager();
		
		AllSegmentsInRangeAcceptingIndexCondition<Long,TestValue> condition = new AllSegmentsInRangeAcceptingIndexCondition<>(collectionMock, null);
		
		try {
			condition.isIn(new HashSet<>());
			fail("This method should be unsupported");
		} catch(UnsupportedOperationException e) { }
		
		try {
			condition.extractIndexValueFromKey(new TimeKey(1, 1));
			fail("This method should be unsupported");
		} catch(UnsupportedOperationException e) { }
		
		try {
			condition.createKeyForIndexValue(10L);
			fail("This method should be unsupported");
		} catch(UnsupportedOperationException e) { }
	}

	@SuppressWarnings("unchecked")
	@Test
	public void test_getSegmentRangesToIncludeInCollectionQuery() throws BlueDbException {
		ReadableCollectionOnDisk<TestValue> collectionMock = (ReadableCollectionOnDisk<TestValue>) Mockito.mock(ReadableCollectionOnDisk.class);
		ReadableSegmentManager<TestValue> segmentManagerMock = (ReadableSegmentManager<TestValue>) Mockito.mock(ReadableSegmentManager.class);
		Mockito.doReturn(segmentManagerMock).when(collectionMock).getSegmentManager();
		
		Range groupingNumberRangeToAccept = new Range(0, 29);
		List<Range> segmentRangesForGroupingNumberRange = Arrays.asList(
				new Range(0, 9),
				new Range(10, 19),
				new Range(20, 29)
				);
		Mockito.doReturn(segmentRangesForGroupingNumberRange).when(segmentManagerMock).getExistingSegmentRanges(groupingNumberRangeToAccept, Optional.empty());

		AllSegmentsInRangeAcceptingIndexCondition<Long,TestValue> condition = new AllSegmentsInRangeAcceptingIndexCondition<>(collectionMock, groupingNumberRangeToAccept);
		assertEquals(new HashSet<>(segmentRangesForGroupingNumberRange), condition.getSegmentRangesToIncludeInCollectionQuery());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void test_test() throws BlueDbException {
		ReadableCollectionOnDisk<TestValue> collectionMock = (ReadableCollectionOnDisk<TestValue>) Mockito.mock(ReadableCollectionOnDisk.class);
		ReadableSegmentManager<TestValue> segmentManagerMock = (ReadableSegmentManager<TestValue>) Mockito.mock(ReadableSegmentManager.class);
		SegmentPathManager segmentPathManagerMock = (SegmentPathManager) Mockito.mock(SegmentPathManager.class);
		
		Mockito.doReturn(segmentPathManagerMock).when(segmentManagerMock).getPathManager();
		Mockito.doReturn(segmentManagerMock).when(collectionMock).getSegmentManager();
		
		Range groupingNumberRangeToAccept = new Range(5, 20);
		
		AllSegmentsInRangeAcceptingIndexCondition<Long,TestValue> condition = new AllSegmentsInRangeAcceptingIndexCondition<>(collectionMock, groupingNumberRangeToAccept);
		assertFalse(condition.test(new BlueEntity<>(new TimeKey(0, 0), new TestValue("Bob"))));
		assertFalse(condition.test(new BlueEntity<>(new TimeKey(4, 4), new TestValue("Bob"))));
		assertTrue(condition.test(new BlueEntity<>(new TimeKey(5, 5), new TestValue("Bob"))));
		assertTrue(condition.test(new BlueEntity<>(new TimeKey(10, 10), new TestValue("Bob"))));
		assertTrue(condition.test(new BlueEntity<>(new TimeKey(20, 20), new TestValue("Bob"))));
		assertFalse(condition.test(new BlueEntity<>(new TimeKey(21, 21), new TestValue("Bob"))));
		assertFalse(condition.test(new BlueEntity<>(new TimeKey(30, 30), new TestValue("Bob"))));
	}
}
