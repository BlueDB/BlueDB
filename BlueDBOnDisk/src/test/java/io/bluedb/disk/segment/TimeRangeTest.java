package io.bluedb.disk.segment;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class TimeRangeTest {

	@Test
	public void test_timeRange() {
		TimeRange _12_24 = new TimeRange(12, 24);
		assertEquals(12, _12_24.getStart());
		assertEquals(24, _12_24.getEnd());
	}

	@Test
	public void test_getStart() {
		TimeRange _12_24 = new TimeRange(12, 24);
		assertEquals(12, _12_24.getStart());
	}

	@Test
	public void test_getEnd() {
		TimeRange _12_24 = new TimeRange(12, 24);
		assertEquals(24, _12_24.getEnd());
	}

	@Test
	public void test_toUnderscoreDelimitedString() {
		TimeRange _12_24 = new TimeRange(12, 24);
		assertEquals("12_24", _12_24.toUnderscoreDelimitedString());
	}

	@Test
	public void test_fromUnderscoreDelmimitedString() {
		TimeRange _12_24 = TimeRange.fromUnderscoreDelmimitedString("12_24");
		assertEquals(12, _12_24.getStart());
		assertEquals(24, _12_24.getEnd());

		TimeRange _null = TimeRange.fromUnderscoreDelmimitedString("12_a");
		assertNull(_null);
	}

	@Test
	public void test_hashCode() {
		Integer _0_0 = new TimeRange(0, 0).hashCode();
		Integer _0_0_copy = new TimeRange(0, 0).hashCode();
		Integer _0_3 = new TimeRange(0, 3).hashCode();
		Integer _1_2 = new TimeRange(1, 2).hashCode();
		
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
		TimeRange _12_24 = new TimeRange(12, 24);
		String string = _12_24.toString();
		assertTrue(string.contains("12"));
		assertTrue(string.contains("24"));
		assertTrue(string.contains(_12_24.getClass().getSimpleName()));
	}

	@Test
	public void test_equalsObject() {
		TimeRange _0_0 = new TimeRange(0, 0);
		TimeRange _0_0_copy = new TimeRange(0, 0);
		TimeRange _0_3 = new TimeRange(0, 3);
		TimeRange _1_2 = new TimeRange(1, 2);
		TimeRange _1_3 = new TimeRange(1, 3);
		
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
		TimeRange _0_0 = new TimeRange(0, 0);
		TimeRange _0_0_copy = new TimeRange(0, 0);
		TimeRange _0_3 = new TimeRange(0, 3);
		TimeRange _1_2 = new TimeRange(1, 2);
		TimeRange _1_4 = new TimeRange(1, 4);
		TimeRange _2_2 = new TimeRange(2, 2);
		
		List<TimeRange> inOrder = Arrays.asList(_0_0, _0_0_copy, _0_3, _1_2, _1_4, _2_2);
		List<TimeRange> outOfOrder = Arrays.asList(_1_4, _0_0_copy, _2_2, _1_2, _0_0, _0_3);
		Collections.sort(outOfOrder);
		assertEquals(inOrder, outOfOrder);
	}
}
