package org.bluedb.disk.segment;

import static org.junit.Assert.*;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.recovery.IndividualChange;

public class SegmentBatchTest {
	
	private static List<Long> ROLLUP_LEVELS = Arrays.asList(1L, 2L, 4L, 8L);
	ReadWriteSegment<TestValue> segment = new ReadWriteSegment<>(Paths.get(""), new Range(0, 7), null, null, ROLLUP_LEVELS);
	private static BlueKey key0At0 = createKey(0, 0);
	private static BlueKey key1At1 = createKey(1, 1);
	private static BlueKey key2At1 = createKey(2, 1);
	private static BlueKey key3At3 = createKey(3, 3);
	private static BlueKey key4At4 = createKey(4, 4);
	private static TestValue value0 = createValue("0");
	private static TestValue value1 = createValue("1");
	private static TestValue value2 = createValue("2");
	private static TestValue value3 = createValue("3");
	private static TestValue value4 = createValue("4");
	private static IndividualChange<TestValue> insert0At0 = IndividualChange.createInsertChange(key0At0, value0);
	private static IndividualChange<TestValue> insert1At1 = IndividualChange.createInsertChange(key1At1, value1);
	private static IndividualChange<TestValue> insert2At1 = IndividualChange.createInsertChange(key2At1, value2);
	private static IndividualChange<TestValue> insert3At3 = IndividualChange.createInsertChange(key3At3, value3);
	private static IndividualChange<TestValue> insert4At4 = IndividualChange.createInsertChange(key4At4, value4);
	private static Range range0to0 = new Range(0, 0);
	private static Range range0to1 = new Range(0, 1);
	private static Range range0to3 = new Range(0, 3);
	private static Range range0to4 = new Range(0, 4);
	private static Range range0to7 = new Range(0, 7);
	private static Range range1to1 = new Range(1, 1);
	private static Range range1to7 = new Range(1, 7);
	private static Range range4to4 = new Range(4, 4);
	private static Range range7to7 = new Range(7, 7);
	List<Range> ranges0to0 = Arrays.asList(range0to0);
	List<Range> ranges1to1 = Arrays.asList(range1to1);
	List<Range> ranges4to4 = Arrays.asList(range4to4);
	List<Range> ranges7to7 = Arrays.asList(range7to7);
	List<IndividualChange<TestValue>> empty = Arrays.asList();
	List<IndividualChange<TestValue>> inserts0 = Arrays.asList(insert0At0);
	List<IndividualChange<TestValue>> inserts1 = Arrays.asList(insert1At1);
	List<IndividualChange<TestValue>> inserts1And2At1 = Arrays.asList(insert1At1, insert2At1);
	List<IndividualChange<TestValue>> inserts0and1and3 = Arrays.asList(insert0At0, insert1At1, insert3At3);
	List<IndividualChange<TestValue>> inserts0and1and4 = Arrays.asList(insert0At0, insert1At1, insert4At4);
	List<IndividualChange<TestValue>> inserts0and1 = Arrays.asList(insert0At0, insert1At1);
	List<IndividualChange<TestValue>> inserts0and4 = Arrays.asList(insert0At0, insert4At4);
	List<IndividualChange<TestValue>> inserts4 = Arrays.asList(insert4At4);
	SegmentBatch<TestValue> batchInsert1and2at1 = new SegmentBatch<>(inserts1And2At1);
	SegmentBatch<TestValue> batchInsert0and1and3 = new SegmentBatch<>(inserts0and1and3);
	SegmentBatch<TestValue> batchInsert0and1and4 = new SegmentBatch<>(inserts0and1and4);
	SegmentBatch<TestValue> batchInsertEmtpy = new SegmentBatch<>(empty);

	@Test
	public void test_breakIntoChunks_cappedByExistingChunk() {
		//            0 1 2 3 4 5 6 7
		// existing: |-|-|-|-|-|-|-|o|
		// proposed: |x|x|-|-|x|-|-|-|
		// expected:  ---     -      
		List<ChunkBatch<TestValue>> chunkBatches = batchInsert0and1and4.breakIntoChunks(ranges7to7, segment);
		assertEquals(2, chunkBatches.size());
		assertEquals(range0to1, chunkBatches.get(0).getRange());
		assertEquals(range4to4, chunkBatches.get(1).getRange());
		assertEquals(inserts0and1, chunkBatches.get(0).getChangesInOrder());
		assertEquals(inserts4, chunkBatches.get(1).getChangesInOrder());
	}


	@Test
	public void test_breakIntoChunks_overlapsExistingChunk() {
		//            0 1 2 3 4 5 6 7
		// existing: |o|-|-|-|-|-|-|-|
		// proposed: |x|x|-|-|x|-|-|-|
		// expected:  - -     -      
		List<ChunkBatch<TestValue>> chunkBatches = batchInsert0and1and4.breakIntoChunks(ranges0to0, segment);
		assertEquals(3, chunkBatches.size());
		assertEquals(range0to0, chunkBatches.get(0).getRange());
		assertEquals(range1to1, chunkBatches.get(1).getRange());
		assertEquals(range4to4, chunkBatches.get(2).getRange());
		assertEquals(inserts0, chunkBatches.get(0).getChangesInOrder());
		assertEquals(inserts1, chunkBatches.get(1).getChangesInOrder());
		assertEquals(inserts4, chunkBatches.get(2).getChangesInOrder());
	}

	@Test
	public void test_breakIntoChunks_empty() {
		//            0 1 2 3 4 5 6 7
		// existing: |-|-|-|-|-|-|-|-|
		// proposed: |-|-|-|-|-|-|-|-|
		// expected:  
		List<ChunkBatch<TestValue>> chunkBatches = batchInsertEmtpy.breakIntoChunks(Arrays.asList(), segment);
		assertEquals(0, chunkBatches.size());
	}

	@Test
	public void test_breakIntoChunks_noExistingChunks() {
		//            0 1 2 3 4 5 6 7
		// existing: |-|-|-|-|-|-|-|-|
		// proposed: |x|x|-|-|x|-|-|-|
		// expected:  ---------------
		List<ChunkBatch<TestValue>> chunkBatches = batchInsert0and1and4.breakIntoChunks(Arrays.asList(), segment);
		assertEquals(1, chunkBatches.size());
		assertEquals(range0to7, chunkBatches.get(0).getRange());
		assertEquals(inserts0and1and4, chunkBatches.get(0).getChangesInOrder());
	}

	@Test
	public void test_breakIntoChunks_noExistingChunks2() {
		//            0 1 2 3 4 5 6 7
		// existing: |-|-|-|-|-|-|-|-|
		// proposed: |x|x|-|x|-|-|-|-|
		// expected:  ---------------
		List<ChunkBatch<TestValue>> chunkBatches = batchInsert0and1and3.breakIntoChunks(Arrays.asList(), segment);
		assertEquals(1, chunkBatches.size());
		assertEquals(range0to3, chunkBatches.get(0).getRange());
		assertEquals(inserts0and1and3, chunkBatches.get(0).getChangesInOrder());
	}

	@Test
	public void test_breakIntoChunks_multipleAtOneGroupingNumber() {
		//            0 1 2 3 4 5 6 7
		// existing: |-|-|-|-|-|-|-|-|
		// proposed: |X|-|-|-|-|-|-|-|
		// expected:  -
		List<ChunkBatch<TestValue>> chunkBatches = batchInsert1and2at1.breakIntoChunks(Arrays.asList(), segment);
		assertEquals(1, chunkBatches.size());
		assertEquals(range1to1, chunkBatches.get(0).getRange());
		assertEquals(inserts1And2At1, chunkBatches.get(0).getChangesInOrder());
	}

	@Test
	public void test_pollChangesBeforeOrAt() {
		LinkedList<IndividualChange<TestValue>> inputs;
		LinkedList<IndividualChange<TestValue>> extracted;

		inputs = new LinkedList<>(inserts0and1and4); 
		extracted = SegmentBatch.pollChangesBeforeOrAt(inputs, -1);
		assertEquals(empty, extracted);
		assertEquals(inserts0and1and4, inputs);

		inputs = new LinkedList<>(inserts0and1and4); 
		extracted = SegmentBatch.pollChangesBeforeOrAt(inputs, 1);
		assertEquals(inserts0and1, extracted);
		assertEquals(inserts4, inputs);

		inputs = new LinkedList<>(inserts0and1and4); 
		extracted = SegmentBatch.pollChangesBeforeOrAt(inputs, 4);
		assertEquals(inserts0and1and4, extracted);
		assertEquals(empty, inputs);
	}

	@Test
	public void test_rangeContainsAll() {
		assertFalse(SegmentBatch.rangeContainsAll(range0to0, inserts0and1and4));
		assertTrue(SegmentBatch.rangeContainsAll(range0to4, inserts0and1and4));
		assertTrue(SegmentBatch.rangeContainsAll(range0to7, inserts0and1and4));
		assertFalse(SegmentBatch.rangeContainsAll(range1to7, inserts0and1and4));
	}

	@Test
	public void test_getLargestEmptyRange() {
		List<Range> optionsSmallToBig = Arrays.asList(
					new Range(1552271577603L, 1552271577603L),
					new Range(1552271574000L, 1552271579999L),
					new Range(1552269600000L, 1552273199999L),
					new Range(1552262400000L, 1552348799999L)
				);
		HashSet<Range> existingChunkRanges = new HashSet<>(Arrays.asList());
		Range selected = SegmentBatch.getLargestEmptyRange(optionsSmallToBig, existingChunkRanges);
		assertEquals(new Range(1552262400000L, 1552348799999L), selected);
	}

	public static BlueKey createKey(long keyId, long time){
		return new TimeKey(keyId, time);
	}

	public static TestValue createValue(String name){
		return new TestValue(name);
	}
}
