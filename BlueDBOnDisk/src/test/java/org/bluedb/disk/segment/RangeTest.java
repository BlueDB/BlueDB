package org.bluedb.disk.segment;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class RangeTest {

	@Test
	public void test_timeRange() {
		Range _12_24 = new Range(12, 24);
		assertEquals(12, _12_24.getStart());
		assertEquals(24, _12_24.getEnd());
	}

	@Test
	public void test_getStart() {
		Range _12_24 = new Range(12, 24);
		assertEquals(12, _12_24.getStart());
	}

	@Test
	public void test_getEnd() {
		Range _12_24 = new Range(12, 24);
		assertEquals(24, _12_24.getEnd());
	}

	@Test
	public void test_length() {
		Range _12_12 = new Range(12, 12);
		Range _12_24 = new Range(12, 24);
		assertEquals(1, _12_12.length());
		assertEquals(13, _12_24.length());
	}

	@Test
	public void test_containsInclusive() {
		Range _12_24 = new Range(12, 24);
		assertFalse(_12_24.containsInclusive(11));
		assertTrue(_12_24.containsInclusive(12));
		assertTrue(_12_24.containsInclusive(18));
		assertTrue(_12_24.containsInclusive(24));
		assertFalse(_12_24.containsInclusive(25));
		assertFalse(_12_24.containsInclusive(Long.MIN_VALUE));
		assertFalse(_12_24.containsInclusive(Long.MAX_VALUE));
	}

	@Test
	public void test_overlaps() {
		Range _10_11 = new Range(10, 11);
		Range _10_13 = new Range(10, 13);
		Range _12_12 = new Range(12, 12);
		Range _12_24 = new Range(12, 24);
		Range _16_20 = new Range(16, 20);
		Range _23_26 = new Range(23, 26);
		Range _24_24 = new Range(24, 24);
		Range _25_26 = new Range(25, 26);
		
		assertFalse(_12_24.overlaps(_10_11));
		assertFalse(_12_24.overlaps(_25_26));
		assertTrue(_12_24.overlaps(_10_13));
		assertTrue(_12_24.overlaps(_23_26));
		assertTrue(_12_24.overlaps(_12_12));
		assertTrue(_12_24.overlaps(_24_24));
		assertTrue(_12_24.overlaps(_10_13));
		assertTrue(_12_24.overlaps(_16_20));
		assertTrue(_16_20.overlaps(_12_24));
	}

	@Test
	public void test_overlapsAny() {
		Range _10_11 = new Range(10, 11);
		Range _12_12 = new Range(12, 12);
		Range _12_24 = new Range(12, 24);
		Range _16_20 = new Range(16, 20);
		Range _25_26 = new Range(25, 26);
		
		assertFalse(_12_24.overlapsAny(Arrays.asList(_10_11, _25_26)));
		assertFalse(_12_24.overlapsAny(Arrays.asList()));
		assertTrue(_12_24.overlapsAny(Arrays.asList(_10_11, _25_26, _16_20)));
		assertTrue(_12_24.overlapsAny(Arrays.asList(_12_12, _16_20)));
	}

	@Test
	public void test_encloses() {
		Range _1_2 = new Range(1, 2);
		Range _2_3 = new Range(2, 3);
		Range _3_4 = new Range(3, 4);
		Range _1_4 = new Range(1, 4);
		Range _0_5 = new Range(0, 5);
		
		assertTrue(_3_4.encloses(_3_4)); // same range
		assertTrue(_1_4.encloses(_1_2)); // encloses on the lower edge
		assertTrue(_1_4.encloses(_3_4)); // encloses on the upper edge
		assertTrue(_0_5.encloses(_1_2)); // encloses in the middle

		assertFalse(_1_2.encloses(_2_3)); // overlap but not enclosing
		assertFalse(_1_2.encloses(_3_4)); // no overlap
		assertFalse(_1_2.encloses(_0_5)); // reversed
	}

	@Test
	public void test_toUnderscoreDelimitedString() {
		Range _12_24 = new Range(12, 24);
		assertEquals("12_24", _12_24.toUnderscoreDelimitedString());
	}

	@Test
	public void test_fromUnderscoreDelmimitedString() {
		Range _12_24 = Range.fromUnderscoreDelmimitedString("12_24");
		assertEquals(12, _12_24.getStart());
		assertEquals(24, _12_24.getEnd());

		Range _null = Range.fromUnderscoreDelmimitedString("12_a");
		assertNull(_null);
	}

	@Test
	public void test_hashCode() {
		Integer _0_0 = new Range(0, 0).hashCode();
		Integer _0_0_copy = new Range(0, 0).hashCode();
		Integer _0_3 = new Range(0, 3).hashCode();
		Integer _1_2 = new Range(1, 2).hashCode();
		
		assertEquals(_0_0, _0_0);
		assertTrue(_0_0.equals(_0_0));
		assertTrue(_0_0.equals(_0_0_copy));
		assertFalse(_0_0.equals(_0_3));
		assertFalse(_0_3.equals(_0_0));
		assertFalse(_1_2.equals(_0_3));
		assertFalse(_0_3.equals(_1_2));
	}

	@Test
	public void test_toString() {
		Range _12_24 = new Range(12, 24);
		String string = _12_24.toString();
		assertTrue(string.contains(_12_24.getClass().getSimpleName()));
		assertTrue(string.contains("12"));
		assertTrue(string.contains("24"));
		assertTrue(string.contains(_12_24.getClass().getSimpleName()));
	}

	@Test
	public void test_equalsObject() {
		Range _0_0 = new Range(0, 0);
		Range _0_0_copy = new Range(0, 0);
		Range _0_3 = new Range(0, 3);
		Range _1_2 = new Range(1, 2);
		Range _1_3 = new Range(1, 3);
		
		assertEquals(_0_0, _0_0);
		assertTrue(_0_0.equals(_0_0));
		assertTrue(_0_0.equals(_0_0_copy));
		assertFalse(_0_0.equals(_0_3));
		assertFalse(_0_3.equals(_0_0));
		assertFalse(_1_2.equals(_0_3));
		assertFalse(_0_3.equals(_1_2));
		assertFalse(_1_3.equals(_0_3));
		assertFalse(_0_3.equals(_1_3));

		assertFalse(_0_0.equals(null));
		assertFalse(_0_0.equals("0"));
	}

	@Test
	public void test_compareTo() {
		Range _0_0 = new Range(0, 0);
		Range _0_0_copy = new Range(0, 0);
		Range _0_3 = new Range(0, 3);
		Range _1_2 = new Range(1, 2);
		Range _1_4 = new Range(1, 4);
		Range _2_2 = new Range(2, 2);
		
		List<Range> inOrder = Arrays.asList(_0_0, _0_0_copy, _0_3, _1_2, _1_4, _2_2);
		List<Range> outOfOrder = Arrays.asList(_1_4, _0_0_copy, _2_2, _1_2, _0_0, _0_3);
		Collections.sort(outOfOrder);
		assertEquals(inOrder, outOfOrder);
	}


	@Test
	public void test_forValueAndRangeSize() {
		Range rangeStartingAtZero = Range.forValueAndRangeSize(0, 100);
		assertEquals(0, rangeStartingAtZero.getStart());
		assertEquals(100 - 1, rangeStartingAtZero.getEnd());

		Range rangeStartingAtMinus100 = Range.forValueAndRangeSize(-100, 100);
		assertEquals(-100, rangeStartingAtMinus100.getStart());
		assertEquals(-1, rangeStartingAtMinus100.getEnd());

		Range maxLongRange = Range.forValueAndRangeSize(Long.MAX_VALUE, 100);
		assertTrue(maxLongRange.getEnd() > maxLongRange.getStart());
		assertEquals(Long.MAX_VALUE, maxLongRange.getEnd());

		Range minLongRange = Range.forValueAndRangeSize(Long.MIN_VALUE, 100);
		assertTrue(minLongRange.getEnd() > minLongRange.getStart());
		assertEquals(Long.MIN_VALUE, minLongRange.getStart());
	}
}
