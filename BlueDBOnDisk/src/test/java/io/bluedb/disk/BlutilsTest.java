package io.bluedb.disk;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class BlutilsTest {

	@Test
	public void test_roundDownToMultiple() {
		assertEquals(0, Blutils.roundDownToMultiple(0, 2));  // test zero
		assertEquals(4, Blutils.roundDownToMultiple(5, 2));  // test greater than a multiple
		assertEquals(0, Blutils.roundDownToMultiple(41, 42));  // test equal to a multiple
		assertEquals(42, Blutils.roundDownToMultiple(42, 42));  // test equal to a multiple
		// TODO test at Long.MAX_VALUE, Long.MIN_VALUE
	}

	@Test
	public void test_filter() {
		Long[] values = new Long[]{7L, 5L, 1000L, 1L};
		Long[] valuesBiggerThan2 = new Long[]{7L, 5L, 1000L};
		List<Long> valuesBiggerThan2List = Arrays.asList(valuesBiggerThan2);
		List<Long> filteredValues = Blutils.filter(values, (l) -> (l > 2));
		assertEquals(valuesBiggerThan2List, filteredValues);
	}
}
