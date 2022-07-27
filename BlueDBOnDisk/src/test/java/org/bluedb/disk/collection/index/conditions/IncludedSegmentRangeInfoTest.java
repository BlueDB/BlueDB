package org.bluedb.disk.collection.index.conditions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.bluedb.disk.StreamUtils;
import org.bluedb.disk.segment.Range;
import org.junit.Test;

public class IncludedSegmentRangeInfoTest {

	@Test
	public void test_isEmpty() {
		IncludedSegmentRangeInfo includedSegmentRangeInfo = new IncludedSegmentRangeInfo();
		assertTrue(includedSegmentRangeInfo.isEmpty());
		
		includedSegmentRangeInfo.addIncludedSegmentRangeInfo(new Range(0, 9), 5);
		assertFalse(includedSegmentRangeInfo.isEmpty());
		
		includedSegmentRangeInfo.addIncludedSegmentRangeInfo(new Range(10, 19), 5);
		
		includedSegmentRangeInfo.removeIncludedSegmentRangeInfo(new Range(0, 9));
		assertFalse(includedSegmentRangeInfo.isEmpty());
		
		includedSegmentRangeInfo.removeIncludedSegmentRangeInfo(new Range(10, 19));
		assertTrue(includedSegmentRangeInfo.isEmpty());
	}

	@Test
	public void test_containsAndGetRangeForSegment() {
		IncludedSegmentRangeInfo includedSegmentRangeInfo = new IncludedSegmentRangeInfo();
		assertFalse(includedSegmentRangeInfo.containsSegment(new Range(0, 9)));
		assertNull(includedSegmentRangeInfo.getRangeForSegment(new Range(0, 9)));
		assertFalse(includedSegmentRangeInfo.containsSegment(new Range(10, 19)));
		assertNull(includedSegmentRangeInfo.getRangeForSegment(new Range(10, 19)));

		includedSegmentRangeInfo.addIncludedSegmentRangeInfo(new Range(0, 9), 5);
		assertTrue(includedSegmentRangeInfo.containsSegment(new Range(0, 9)));
		assertEquals(new Range(5, 5), includedSegmentRangeInfo.getRangeForSegment(new Range(0, 9)));
		assertFalse(includedSegmentRangeInfo.containsSegment(new Range(10, 19)));
		assertNull(includedSegmentRangeInfo.getRangeForSegment(new Range(10, 19)));
	}
	
	@Test
	public void test_copyConstructor() {
		IncludedSegmentRangeInfo includedSegmentRangeInfo1 = new IncludedSegmentRangeInfo();
		includedSegmentRangeInfo1.addIncludedSegmentRangeInfo(new Range(0, 9), new Range(2, 8));
		includedSegmentRangeInfo1.addIncludedSegmentRangeInfo(new Range(10, 19), new Range(12, 18));
		
		IncludedSegmentRangeInfo includedSegmentRangeInfo2 = new IncludedSegmentRangeInfo(includedSegmentRangeInfo1);
		assertEquals(new Range(2, 8), includedSegmentRangeInfo2.getRangeForSegment(new Range(0, 9)));
		assertEquals(new Range(12, 18), includedSegmentRangeInfo2.getRangeForSegment(new Range(10, 19)));
		
		//Removing things from the first object shouldn't affect the second object
		includedSegmentRangeInfo1.removeIncludedSegmentRangeInfo(new Range(0, 9));
		includedSegmentRangeInfo1.removeIncludedSegmentRangeInfo(new Range(10, 19));
		assertTrue(includedSegmentRangeInfo1.isEmpty());
		assertEquals(new Range(2, 8), includedSegmentRangeInfo2.getRangeForSegment(new Range(0, 9)));
		assertEquals(new Range(12, 18), includedSegmentRangeInfo2.getRangeForSegment(new Range(10, 19)));
	}
	
	@Test
	public void test_getSegmentRangeGroupingNumberRangePairs() {
		IncludedSegmentRangeInfo includedSegmentRangeInfo1 = new IncludedSegmentRangeInfo();
		includedSegmentRangeInfo1.addIncludedSegmentRangeInfo(new Range(0, 9), new Range(2, 8));
		includedSegmentRangeInfo1.addIncludedSegmentRangeInfo(new Range(10, 19), new Range(12, 18));
		
		LinkedList<Entry<Range, Range>> entities = StreamUtils.stream(includedSegmentRangeInfo1.getSegmentRangeGroupingNumberRangePairs())
			.sorted(Comparator.comparing(Entry::getKey))
			.collect(Collectors.toCollection(LinkedList::new));
		
		assertEquals(new Range(0, 9), entities.get(0).getKey());
		assertEquals(new Range(2, 8), entities.get(0).getValue());
		assertEquals(new Range(10, 19), entities.get(1).getKey());
		assertEquals(new Range(12, 18), entities.get(1).getValue());
	}
	
	@Test
	public void test_addIncludedSegmentRangeInfo_long() {
		IncludedSegmentRangeInfo includedSegmentRangeInfo = new IncludedSegmentRangeInfo();
		
		includedSegmentRangeInfo.addIncludedSegmentRangeInfo(new Range(0, 9), 5);
		assertEquals(new Range(5, 5), includedSegmentRangeInfo.getRangeForSegment(new Range(0, 9)));
		
		includedSegmentRangeInfo.addIncludedSegmentRangeInfo(new Range(0, 9), 6);
		assertEquals(new Range(5, 6), includedSegmentRangeInfo.getRangeForSegment(new Range(0, 9)));
		
		includedSegmentRangeInfo.addIncludedSegmentRangeInfo(new Range(0, 9), 3);
		assertEquals(new Range(3, 6), includedSegmentRangeInfo.getRangeForSegment(new Range(0, 9)));
		
		includedSegmentRangeInfo.addIncludedSegmentRangeInfo(new Range(0, 9), 23);
		assertEquals(new Range(3, 23), includedSegmentRangeInfo.getRangeForSegment(new Range(0, 9)));
		
		includedSegmentRangeInfo.addIncludedSegmentRangeInfo(new Range(0, 9), 5);
		assertEquals(new Range(3, 23), includedSegmentRangeInfo.getRangeForSegment(new Range(0, 9)));
	}
	
	@Test
	public void test_addIncludedSegmentRangeInfo_range() {
		IncludedSegmentRangeInfo includedSegmentRangeInfo = new IncludedSegmentRangeInfo();
		
		includedSegmentRangeInfo.addIncludedSegmentRangeInfo(new Range(0, 9), new Range(4, 6));
		assertEquals(new Range(4, 6), includedSegmentRangeInfo.getRangeForSegment(new Range(0, 9)));
		
		includedSegmentRangeInfo.addIncludedSegmentRangeInfo(new Range(0, 9), new Range(10, 12));
		assertEquals(new Range(4, 12), includedSegmentRangeInfo.getRangeForSegment(new Range(0, 9)));
		
		includedSegmentRangeInfo.addIncludedSegmentRangeInfo(new Range(0, 9), new Range(3, 5));
		assertEquals(new Range(3, 12), includedSegmentRangeInfo.getRangeForSegment(new Range(0, 9)));
		
		includedSegmentRangeInfo.addIncludedSegmentRangeInfo(new Range(0, 9), new Range(5, 5));
		assertEquals(new Range(3, 12), includedSegmentRangeInfo.getRangeForSegment(new Range(0, 9)));
	}
	
	@Test
	public void test_combine_and() {
		IncludedSegmentRangeInfo includedSegmentRangeInfo1 = new IncludedSegmentRangeInfo();
		includedSegmentRangeInfo1.addIncludedSegmentRangeInfo(new Range(0, 9), new Range(3, 7));
		includedSegmentRangeInfo1.addIncludedSegmentRangeInfo(new Range(10, 19), new Range(3, 7));
		includedSegmentRangeInfo1.addIncludedSegmentRangeInfo(new Range(20, 29), new Range(3, 7));
		
		IncludedSegmentRangeInfo includedSegmentRangeInfo2 = new IncludedSegmentRangeInfo();
		includedSegmentRangeInfo2.addIncludedSegmentRangeInfo(new Range(0, 9), new Range(10, 12));
		includedSegmentRangeInfo2.addIncludedSegmentRangeInfo(new Range(10, 19), new Range(5, 10));
		includedSegmentRangeInfo2.addIncludedSegmentRangeInfo(new Range(20, 29), new Range(4, 6));
		
		includedSegmentRangeInfo1.combine(includedSegmentRangeInfo2, true);
		assertFalse("If the ranges don't overlap then the segment is removed", includedSegmentRangeInfo1.containsSegment(new Range(0, 9)));
		assertEquals("Only the overlapping range should survive", new Range(5, 7), includedSegmentRangeInfo1.getRangeForSegment(new Range(10, 19)));
		assertEquals("Only the overlapping range should survive", new Range(4, 6), includedSegmentRangeInfo1.getRangeForSegment(new Range(20, 29)));
	}
	
	@Test
	public void test_combine_andWithNull() {
		IncludedSegmentRangeInfo includedSegmentRangeInfo1 = new IncludedSegmentRangeInfo();
		includedSegmentRangeInfo1.addIncludedSegmentRangeInfo(new Range(0, 9), new Range(3, 7));
		includedSegmentRangeInfo1.addIncludedSegmentRangeInfo(new Range(10, 19), new Range(3, 7));
		includedSegmentRangeInfo1.addIncludedSegmentRangeInfo(new Range(20, 29), new Range(3, 7));
		
		includedSegmentRangeInfo1.combine(null, true);
		assertTrue("Anding with a null clears the original info", includedSegmentRangeInfo1.isEmpty());
	}
	
	@Test
	public void test_combine_andWithEmpty() {
		IncludedSegmentRangeInfo includedSegmentRangeInfo1 = new IncludedSegmentRangeInfo();
		includedSegmentRangeInfo1.addIncludedSegmentRangeInfo(new Range(0, 9), new Range(3, 7));
		includedSegmentRangeInfo1.addIncludedSegmentRangeInfo(new Range(10, 19), new Range(3, 7));
		includedSegmentRangeInfo1.addIncludedSegmentRangeInfo(new Range(20, 29), new Range(3, 7));
		
		IncludedSegmentRangeInfo includedSegmentRangeInfo2 = new IncludedSegmentRangeInfo();
		
		includedSegmentRangeInfo1.combine(includedSegmentRangeInfo2, true);
		assertTrue("Anding with an empty info clears the original info", includedSegmentRangeInfo1.isEmpty());
	}
	
	@Test
	public void test_combine_or() {
		IncludedSegmentRangeInfo includedSegmentRangeInfo1 = new IncludedSegmentRangeInfo();
		includedSegmentRangeInfo1.addIncludedSegmentRangeInfo(new Range(0, 9), new Range(3, 7));
		includedSegmentRangeInfo1.addIncludedSegmentRangeInfo(new Range(10, 19), new Range(3, 7));
		includedSegmentRangeInfo1.addIncludedSegmentRangeInfo(new Range(20, 29), new Range(3, 7));
		
		IncludedSegmentRangeInfo includedSegmentRangeInfo2 = new IncludedSegmentRangeInfo();
		includedSegmentRangeInfo2.addIncludedSegmentRangeInfo(new Range(0, 9), new Range(10, 12));
		includedSegmentRangeInfo2.addIncludedSegmentRangeInfo(new Range(10, 19), new Range(5, 10));
		includedSegmentRangeInfo2.addIncludedSegmentRangeInfo(new Range(20, 29), new Range(4, 6));
		
		includedSegmentRangeInfo1.combine(includedSegmentRangeInfo2, false);
		assertEquals(new Range(3, 12), includedSegmentRangeInfo1.getRangeForSegment(new Range(0, 9)));
		assertEquals(new Range(3, 10), includedSegmentRangeInfo1.getRangeForSegment(new Range(10, 19)));
		assertEquals(new Range(3, 7), includedSegmentRangeInfo1.getRangeForSegment(new Range(20, 29)));
	}
	
	@Test
	public void test_combine_orWithNull() {
		IncludedSegmentRangeInfo includedSegmentRangeInfo1 = new IncludedSegmentRangeInfo();
		includedSegmentRangeInfo1.addIncludedSegmentRangeInfo(new Range(0, 9), new Range(3, 7));
		includedSegmentRangeInfo1.addIncludedSegmentRangeInfo(new Range(10, 19), new Range(3, 7));
		includedSegmentRangeInfo1.addIncludedSegmentRangeInfo(new Range(20, 29), new Range(3, 7));
		
		includedSegmentRangeInfo1.combine(null, false);
		assertEquals(new Range(3, 7), includedSegmentRangeInfo1.getRangeForSegment(new Range(0, 9)));
		assertEquals(new Range(3, 7), includedSegmentRangeInfo1.getRangeForSegment(new Range(10, 19)));
		assertEquals(new Range(3, 7), includedSegmentRangeInfo1.getRangeForSegment(new Range(20, 29)));
	}
	
	@Test
	public void test_combine_orWithEmpty() {
		IncludedSegmentRangeInfo includedSegmentRangeInfo1 = new IncludedSegmentRangeInfo();
		includedSegmentRangeInfo1.addIncludedSegmentRangeInfo(new Range(0, 9), new Range(3, 7));
		includedSegmentRangeInfo1.addIncludedSegmentRangeInfo(new Range(10, 19), new Range(3, 7));
		includedSegmentRangeInfo1.addIncludedSegmentRangeInfo(new Range(20, 29), new Range(3, 7));
		
		IncludedSegmentRangeInfo includedSegmentRangeInfo2 = new IncludedSegmentRangeInfo();
		
		includedSegmentRangeInfo1.combine(includedSegmentRangeInfo2, false);
		assertEquals(new Range(3, 7), includedSegmentRangeInfo1.getRangeForSegment(new Range(0, 9)));
		assertEquals(new Range(3, 7), includedSegmentRangeInfo1.getRangeForSegment(new Range(10, 19)));
		assertEquals(new Range(3, 7), includedSegmentRangeInfo1.getRangeForSegment(new Range(20, 29)));
	}
	
	@Test
	public void test_toString() {
		//Just don't want it to be flagged as not covered
		new IncludedSegmentRangeInfo().toString();
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void test_hashCodeAndEquals() {
		IncludedSegmentRangeInfo nullInfo = null;
		
		IncludedSegmentRangeInfo emptyInfo = new IncludedSegmentRangeInfo();
		
		IncludedSegmentRangeInfo info1 = new IncludedSegmentRangeInfo();
		info1.addIncludedSegmentRangeInfo(new Range(0, 9), 5);
		info1.addIncludedSegmentRangeInfo(new Range(10, 19), 12);
		
		IncludedSegmentRangeInfo info1Copy = new IncludedSegmentRangeInfo(info1);
		
		IncludedSegmentRangeInfo info2 = new IncludedSegmentRangeInfo();
		info2.addIncludedSegmentRangeInfo(new Range(0, 9), 5);
		
		IncludedSegmentRangeInfo info3 = new IncludedSegmentRangeInfo();
		info3.addIncludedSegmentRangeInfo(new Range(0, 9), 12);
		info3.addIncludedSegmentRangeInfo(new Range(10, 19), 5);
		
		assertFalse(info1.equals(nullInfo));
		assertFalse(info1.equals(emptyInfo));
		assertFalse(info1.equals(new String("Wrong class type")));
		assertTrue(info1.equals(info1));
		assertTrue(info1.equals(info1Copy));
		assertEquals(info1.hashCode(), info1Copy.hashCode());
		assertFalse(info1.equals(info2));
		assertFalse(info1.equals(info3));
	}
}
