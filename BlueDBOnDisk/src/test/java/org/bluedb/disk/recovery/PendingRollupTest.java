package org.bluedb.disk.recovery;

import static org.junit.Assert.*;

import java.io.Serializable;

import org.junit.Test;

public class PendingRollupTest {

	@Test
	public void test_toString() {
		long segmentGroupingNumber = 123;
		long min = 345;
		long max = 567;
		PendingRollup<?> rollup = new PendingRollup<Serializable>(segmentGroupingNumber, min, max);
		String rollupString = rollup.toString();
		assertTrue(rollupString.contains(String.valueOf(segmentGroupingNumber)));
		assertTrue(rollupString.contains(String.valueOf(min)));
		assertTrue(rollupString.contains(String.valueOf(max)));
	}
}
