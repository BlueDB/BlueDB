package org.bluedb.disk.segment.rollup;

import static org.junit.Assert.*;
import org.junit.Test;
import org.bluedb.disk.segment.Range;

public class RollupTargetTest {

	@Test
	public void test_getWriteRollupDelay() {
		Range rangeA = new Range(1,2);
		Range rangeB = new Range(1,3);
		RollupTarget targetA1 = new RollupTarget(1, rangeA);
		RollupTarget targetA2 = new RollupTarget(2, rangeA);
		RollupTarget targetB1 = new RollupTarget(1, rangeB);
		RollupTarget targetB2 = new RollupTarget(2, rangeB);
		assertEquals(2, targetA1.getWriteRollupDelay());
		assertEquals(2, targetA2.getWriteRollupDelay());
		assertEquals(3, targetB1.getWriteRollupDelay());
		assertEquals(3, targetB2.getWriteRollupDelay());
	}

	@Test
	public void test_getReadRollupDelay() {
		Range rangeA = new Range(1,2);
		Range rangeB = new Range(1,3);
		RollupTarget targetA1 = new RollupTarget(1, rangeA);
		RollupTarget targetA2 = new RollupTarget(2, rangeA);
		RollupTarget targetB1 = new RollupTarget(1, rangeB);
		RollupTarget targetB2 = new RollupTarget(2, rangeB);
		assertEquals(2, targetA1.getReadRollupDelay());
		assertEquals(2, targetA2.getReadRollupDelay());
		assertEquals(3, targetB1.getReadRollupDelay());
		assertEquals(3, targetB2.getReadRollupDelay());
	}

	@Test
	public void test_equals() {
		Range rangeA = new Range(1,2);
		Range rangeB = new Range(1,3);
		RollupTarget target1A = new RollupTarget(1, rangeA);
		RollupTarget target1A_copy = new RollupTarget(1, rangeA);
		RollupTarget target1B = new RollupTarget(1, rangeB);
		RollupTarget target2A = new RollupTarget(2, rangeA);
		RollupTarget target1null = new RollupTarget(1, null);
		
		assertEquals(target1A, target1A_copy);
		assertTrue(target1A.equals(target1A_copy));
		assertFalse(target1A.equals(target1B));
		assertFalse(target1A.equals(target2A));
		assertFalse(target1A.equals(target1null));
		assertFalse(target1A.equals(null));
	}

	@Test
	public void test_toString() {
		long groupingNumber = 16;
		Range range = new Range(24, 31);
		RollupTarget target = new RollupTarget(groupingNumber, range);
		String string = target.toString();
		assertTrue(string.contains(target.getClass().getSimpleName()));
		assertTrue(string.contains(String.valueOf(groupingNumber)));
		assertTrue(string.contains(String.valueOf(range.getStart())));
		assertTrue(string.contains(String.valueOf(range.getEnd())));
	}

	@Test
	public void test_hashCode() {
		Range rangeA = new Range(1,2);
		Range rangeB = new Range(1,3);
		RollupTarget target1A = new RollupTarget(1, rangeA);
		RollupTarget target1A_copy = new RollupTarget(1, rangeA);
		RollupTarget target1B = new RollupTarget(1, rangeB);
		RollupTarget target2A = new RollupTarget(2, rangeA);
		RollupTarget target1null = new RollupTarget(1, null);
		
		assertTrue(target1A.hashCode() == target1A_copy.hashCode());
		assertFalse(target1A.hashCode() == target1B.hashCode());
		assertFalse(target1A.hashCode() == target2A.hashCode());
		assertFalse(target1A.hashCode() == target1null.hashCode());
	}
}
