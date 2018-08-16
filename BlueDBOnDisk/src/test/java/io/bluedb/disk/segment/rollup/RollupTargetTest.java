package io.bluedb.disk.segment.rollup;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.bluedb.disk.segment.Range;

public class RollupTargetTest {
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
