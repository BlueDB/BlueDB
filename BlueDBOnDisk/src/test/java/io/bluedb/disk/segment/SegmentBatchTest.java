package io.bluedb.disk.segment;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.recovery.IndividualChange;

public class SegmentBatchTest {
	
	private static List<Long> ROLLUP_LEVELS = Arrays.asList(1L, 2L, 4L, 8L);
	private static BlueKey key0At0 = createKey(0, 0);
	private static BlueKey key1At1 = createKey(1, 1);
//	private static BlueKey key2At2 = createKey(2, 2);
	private static BlueKey key4At4 = createKey(4, 4);
//	private static BlueKey key7At7 = createKey(7, 7);
	private static TestValue value0 = createValue("0");
	private static TestValue value1 = createValue("1");
//	private static TestValue value2 = createValue("2");
	private static TestValue value4 = createValue("4");
//	private static TestValue value7 = createValue("7");
	private static IndividualChange<TestValue> insert0At0 = IndividualChange.insert(key0At0, value0);
	private static IndividualChange<TestValue> insert1At1 = IndividualChange.insert(key1At1, value1);
//	private static IndividualChange<TestValue> insert2At2 = IndividualChange.insert(key2At2, value2);
	private static IndividualChange<TestValue> insert4At4 = IndividualChange.insert(key4At4, value4);
	private static Range range0to0 = new Range(0, 0);
	private static Range range0to1 = new Range(0, 1);
	private static Range range0to7 = new Range(0, 7);
	private static Range range1to1 = new Range(1, 1);
	private static Range range4to4 = new Range(4, 4);
	private static Range range7to7 = new Range(7, 7);
	List<Range> ranges0to0 = Arrays.asList(range0to0);
	List<Range> ranges1to1 = Arrays.asList(range1to1);
	List<Range> ranges4to4 = Arrays.asList(range4to4);
	List<Range> ranges7to7 = Arrays.asList(range7to7);
	List<IndividualChange<TestValue>> inserts0 = Arrays.asList(insert0At0);
	List<IndividualChange<TestValue>> inserts1 = Arrays.asList(insert1At1);
	List<IndividualChange<TestValue>> inserts0and1and4 = Arrays.asList(insert0At0, insert1At1, insert4At4);
	List<IndividualChange<TestValue>> inserts0and1 = Arrays.asList(insert0At0, insert1At1);
	List<IndividualChange<TestValue>> inserts4 = Arrays.asList(insert4At4);
	SegmentBatch<TestValue> batchInsert0and1and4 = new SegmentBatch<>(inserts0and1and4);

	@Test
	public void test_breakIntoChunks_cappedByExistingChunk() {
		//            0 1 2 3 4 5 6 7
		// existing: |-|-|-|-|-|-|-|o|
		// proposed: |x|x|-|-|x|-|-|-|
		// expected:  ---     -      
		List<ChunkBatch<TestValue>> chunkBatches = batchInsert0and1and4.breakIntoChunks(ranges7to7, ROLLUP_LEVELS);
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
		// expected:  ---     -      
		List<ChunkBatch<TestValue>> chunkBatches = batchInsert0and1and4.breakIntoChunks(ranges0to0, ROLLUP_LEVELS);
		assertEquals(3, chunkBatches.size());
		assertEquals(range0to0, chunkBatches.get(0).getRange());
		assertEquals(range1to1, chunkBatches.get(1).getRange());
		assertEquals(range4to4, chunkBatches.get(2).getRange());
		assertEquals(inserts0, chunkBatches.get(0).getChangesInOrder());
		assertEquals(inserts1, chunkBatches.get(1).getChangesInOrder());
		assertEquals(inserts4, chunkBatches.get(2).getChangesInOrder());
	}

	@Test
	public void test_breakIntoChunks_noExistingChunks() {
		//            0 1 2 3 4 5 6 7
		// existing: |-|-|-|-|-|-|-|-|
		// proposed: |x|x|-|-|x|-|-|-|
		// expected:  ---------------
		List<ChunkBatch<TestValue>> chunkBatches = batchInsert0and1and4.breakIntoChunks(Arrays.asList(), ROLLUP_LEVELS);
		assertEquals(1, chunkBatches.size());
		assertEquals(range0to7, chunkBatches.get(0).getRange());
		assertEquals(inserts0and1and4, chunkBatches.get(0).getChangesInOrder());
	}

	public static BlueKey createKey(long keyId, long time){
		return new TimeKey(keyId, time);
	}

	public static TestValue createValue(String name){
		return new TestValue(name);
	}
}
