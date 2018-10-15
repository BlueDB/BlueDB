package io.bluedb.disk.segment.rollup;

import static org.junit.Assert.*;
import org.junit.Test;
import io.bluedb.disk.segment.Range;

public class IndexRollupTargetTest {

	@Test
	public void test_equals() {
		String indexName = "indexName";
		String wrongIndexName = "wrongIndexName";
		Range rangeA = new Range(1,2);
		Range rangeB = new Range(1,3);
		IndexRollupTarget target1A = new IndexRollupTarget(indexName, 1, rangeA);
		IndexRollupTarget target1A_copy = new IndexRollupTarget(indexName, 1, rangeA);
		RollupTarget nonIndex1A = new RollupTarget(1, rangeA);
		IndexRollupTarget wrongName1A = new IndexRollupTarget(wrongIndexName, 1, rangeA);
		IndexRollupTarget target1B = new IndexRollupTarget(indexName, 1, rangeB);
		IndexRollupTarget target2A = new IndexRollupTarget(indexName, 2, rangeA);
		IndexRollupTarget target1null = new IndexRollupTarget(indexName, 1, null);
		IndexRollupTarget nullIndexTarget = new IndexRollupTarget(null, 1, rangeA);
		
		assertEquals(target1A, target1A_copy);
		assertTrue(target1A.equals(target1A_copy));
		assertFalse(target1A.equals(target1B));
		assertFalse(target1A.equals(nonIndex1A));
		assertFalse(nonIndex1A.equals(target1A));
		assertFalse(target1A.equals(wrongName1A));
		assertFalse(wrongName1A.equals(target1A));
		assertFalse(target1A.equals(target2A));
		assertFalse(target1A.equals(target1null));
		assertFalse(target1A.equals(null));
		assertFalse(target1A.equals(nullIndexTarget));
		assertFalse(nullIndexTarget.equals(target1A));
	}

	@Test
	public void test_hashCode() {
		String indexName = "indexName";
		String wrongIndexName = "wrongIndexName";
		Range rangeA = new Range(1,2);
		Range rangeB = new Range(1,3);
		IndexRollupTarget target1A = new IndexRollupTarget(indexName, 1, rangeA);
		IndexRollupTarget target1A_copy = new IndexRollupTarget(indexName, 1, rangeA);
		RollupTarget nonIndex1A = new RollupTarget(1, rangeA);
		IndexRollupTarget wrongName1A = new IndexRollupTarget(wrongIndexName, 1, rangeA);
		IndexRollupTarget target1B = new IndexRollupTarget(indexName, 1, rangeB);
		IndexRollupTarget target2A = new IndexRollupTarget(indexName, 2, rangeA);
		IndexRollupTarget target1null = new IndexRollupTarget(indexName, 1, null);
		IndexRollupTarget nullIndexTarget = new IndexRollupTarget(null, 1, rangeA);
		
		assertTrue(target1A.hashCode() == target1A_copy.hashCode());
		assertFalse(target1A.hashCode() == nonIndex1A.hashCode());
		assertFalse(target1A.hashCode() == wrongName1A.hashCode());
		assertFalse(target1A.hashCode() == target1B.hashCode());
		assertFalse(target1A.hashCode() == target2A.hashCode());
		assertFalse(target1A.hashCode() == target1null.hashCode());
		assertFalse(target1A.hashCode() == nullIndexTarget.hashCode());
	}

	@Test
	public void test_toString() {
		String indexName = "indexName";
		long groupingNumber = 16;
		Range range = new Range(24, 31);
		IndexRollupTarget target = new IndexRollupTarget(indexName, groupingNumber, range);
		String string = target.toString();
		assertTrue(string.contains(target.getClass().getSimpleName()));
		assertTrue(string.contains(indexName));
		assertTrue(string.contains(String.valueOf(groupingNumber)));
		assertTrue(string.contains(String.valueOf(range.getStart())));
		assertTrue(string.contains(String.valueOf(range.getEnd())));
	}
}
