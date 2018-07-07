package io.bluedb.disk.segment;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class TimeRangeTest {

	@Test
	public void test_hashCode() {
		// TODO
	}

	@Test
	public void test_toString() {
		// TODO
	}

	@Test
	public void test_timeRange() {
		// TODO
	}

	@Test
	public void test_getStart() {
		// TODO
	}

	@Test
	public void test_getEnd() {
		// TODO
	}

	@Test
	public void test_toUnderscoreDelimitedString() {
		// TODO
	}

	@Test
	public void test_fromUnderscoreDelmimitedString() {
		// TODO
	}

	@Test
	public void test_equalsObject() {
		// TODO
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
