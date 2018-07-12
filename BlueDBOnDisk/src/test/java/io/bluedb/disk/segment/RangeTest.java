package io.bluedb.disk.segment;

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
}
