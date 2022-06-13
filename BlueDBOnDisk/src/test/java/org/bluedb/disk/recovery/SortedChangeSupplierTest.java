package org.bluedb.disk.recovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.segment.Range;
import org.junit.Before;
import org.junit.Test;

public abstract class SortedChangeSupplierTest {
	
	protected static final TimeFrameKey key1_1To50 = new TimeFrameKey(1, 1, 50);
	protected static final TimeFrameKey key2_10To60 = new TimeFrameKey(2, 10, 60);
	protected static final TimeFrameKey key3_10To50 = new TimeFrameKey(3, 10, 50);
	protected static final TimeFrameKey key4_15To75 = new TimeFrameKey(4, 15, 75);
	protected static final TimeFrameKey key5_25To28 = new TimeFrameKey(5, 25, 28);
	protected static final TimeFrameKey key6_27To110 = new TimeFrameKey(6, 27, 110);
	protected static final TimeFrameKey key7_32To90 = new TimeFrameKey(7, 32, 90);
	protected static final TimeFrameKey key8_150To165 = new TimeFrameKey(8, 150, 165);
	
	protected static final TestValue value1 = new TestValue("Jeremy", 1);
	protected static final TestValue value2 = new TestValue("Ben", 2);
	protected static final TestValue value3 = new TestValue("Derek", 3);
	protected static final TestValue value4 = new TestValue("David", 4);
	protected static final TestValue value5 = new TestValue("Shawn", 5);
	protected static final TestValue value6 = new TestValue("Andrew", 6);
	protected static final TestValue value7 = new TestValue("Sam", 7);
	protected static final TestValue value8 = new TestValue("Jared", 8);
	
	protected static final IndividualChange<TestValue> change1_1To50 = IndividualChange.manuallyCreateTestChange(key1_1To50, value1, value1.cloneWithNewCupcakeCount(10), false);
	protected static final IndividualChange<TestValue> change2_10to60 = IndividualChange.manuallyCreateTestChange(key2_10To60, value2, value2.cloneWithNewCupcakeCount(20), false);
	protected static final IndividualChange<TestValue> change3_10To50 = IndividualChange.manuallyCreateTestChange(key3_10To50, null, value3, false); //Insertion
	protected static final IndividualChange<TestValue> change4_15_to75 = IndividualChange.manuallyCreateTestChange(key4_15To75, value4, value4.cloneWithNewCupcakeCount(40), false);
	protected static final IndividualChange<TestValue> change5_25to28 = IndividualChange.manuallyCreateTestChange(key5_25To28, value5, null, false); //Deletion
	protected static final IndividualChange<TestValue> change6_27to110 = IndividualChange.manuallyCreateTestChange(key6_27To110, value6, value6.cloneWithNewCupcakeCount(60), false);
	protected static final IndividualChange<TestValue> change7_32_to_90 = IndividualChange.manuallyCreateTestChange(key7_32To90, value7, value7.cloneWithNewCupcakeCount(70), false);
	protected static final IndividualChange<TestValue> change8_150to165 = IndividualChange.manuallyCreateTestChange(key8_150To165, value8, value8.cloneWithNewCupcakeCount(80), false);
	
	protected static final List<IndividualChange<TestValue>> changeList = Arrays.asList(change1_1To50, change2_10to60, change3_10To50, change4_15_to75, change5_25to28, change6_27to110, change7_32_to_90, change8_150to165);
	
	protected SortedChangeSupplier<? extends Serializable> sortedChangeSupplier;
	
	protected abstract SortedChangeSupplier<TestValue> createSortedChangeSupplier() throws Exception;
	
	@Before
	public void setup() throws Exception {
		this.sortedChangeSupplier = createSortedChangeSupplier();
	}
	
	@Test
	public void test_basicIterationThroughAllChangesWorks() throws BlueDbException {
		Range allInclusiveRange = new Range(Long.MIN_VALUE, Long.MAX_VALUE);
		
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(allInclusiveRange));
		assertEquals(change1_1To50, sortedChangeSupplier.getNextChange().orElse(null));
		
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(allInclusiveRange));
		assertEquals(change2_10to60, sortedChangeSupplier.getNextChange().orElse(null));
		
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(allInclusiveRange));
		assertEquals(change3_10To50, sortedChangeSupplier.getNextChange().orElse(null));
		
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(allInclusiveRange));
		assertEquals(change4_15_to75, sortedChangeSupplier.getNextChange().orElse(null));
		
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(allInclusiveRange));
		assertEquals(change5_25to28, sortedChangeSupplier.getNextChange().orElse(null));
		
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(allInclusiveRange));
		assertEquals(change6_27to110, sortedChangeSupplier.getNextChange().orElse(null));
		
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(allInclusiveRange));
		assertEquals(change7_32_to_90, sortedChangeSupplier.getNextChange().orElse(null));
		
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(allInclusiveRange));
		assertEquals(change8_150to165, sortedChangeSupplier.getNextChange().orElse(null));
		
		assertFalse(sortedChangeSupplier.seekToNextChangeInRange(allInclusiveRange));
		
		//Cursor should still be pointed at nothing when it reaches the end
		assertEquals(null, sortedChangeSupplier.getNextChange().orElse(null));
	}
	
	@Test
	public void test_iterationByRangeWorks() throws BlueDbException {
		Range range = new Range(0, 9);
		
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change1_1To50, sortedChangeSupplier.getNextChange().orElse(null));
		assertFalse(sortedChangeSupplier.hasMoreThanOneChangeLeftInRange(range));
		assertFalse(sortedChangeSupplier.seekToNextChangeInRange(range));
		
		
		range = new Range(10, 19);
		
		assertEquals(change2_10to60, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.hasMoreThanOneChangeLeftInRange(range));
		
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change3_10To50, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.hasMoreThanOneChangeLeftInRange(range));
		
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change4_15_to75, sortedChangeSupplier.getNextChange().orElse(null));
		assertFalse(sortedChangeSupplier.hasMoreThanOneChangeLeftInRange(range));
		
		assertFalse(sortedChangeSupplier.seekToNextChangeInRange(range));
		
		
		range = new Range(20, 29);

		assertEquals(change5_25to28, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.hasMoreThanOneChangeLeftInRange(range));

		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change6_27to110, sortedChangeSupplier.getNextChange().orElse(null));
		assertFalse(sortedChangeSupplier.hasMoreThanOneChangeLeftInRange(range));

		assertFalse(sortedChangeSupplier.seekToNextChangeInRange(range));
		
		
		range = new Range(30, 39);
		
		assertEquals(change7_32_to_90, sortedChangeSupplier.getNextChange().orElse(null));
		assertFalse(sortedChangeSupplier.hasMoreThanOneChangeLeftInRange(range));
		
		assertFalse(sortedChangeSupplier.seekToNextChangeInRange(range));
		
		
		range = new Range(40, 49);
		
		assertEquals(change8_150to165, sortedChangeSupplier.getNextChange().orElse(null)); //It is already pointed at the last change
	}
	
	@Test
	public void test_seekToSpecificRange() throws BlueDbException {
		Range range = new Range(75, 120);
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change4_15_to75, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change6_27to110, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change7_32_to_90, sortedChangeSupplier.getNextChange().orElse(null));
		assertFalse(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change8_150to165, sortedChangeSupplier.getNextChange().orElse(null)); //Should point at first change outside the range
		
	}
	
	@Test
	public void test_findGroupingNumbers() throws BlueDbException {
		Set<Long> allGroupingNumbers = new HashSet<>(Arrays.asList((long)1, (long)10, (long)15, (long)25, (long)27, (long)32, (long)150));
		Set<Long> allGroupingNumbersBeforeOrAt27 = new HashSet<>(Arrays.asList((long)1, (long)10, (long)15, (long)25, (long)27));
		Set<Long> allGroupingNumbersAfter10BeforeOrAt32 = new HashSet<>(Arrays.asList((long)15, (long)25, (long)27, (long)32));
		
		assertEquals(allGroupingNumbers, sortedChangeSupplier.findGroupingNumbersForNextChangesBeforeOrAtGroupingNumber(Long.MAX_VALUE));
		assertEquals(allGroupingNumbersBeforeOrAt27, sortedChangeSupplier.findGroupingNumbersForNextChangesBeforeOrAtGroupingNumber(27));
		
		Range allInclusiveRange = new Range(Long.MIN_VALUE, Long.MAX_VALUE);
		sortedChangeSupplier.seekToNextChangeInRange(allInclusiveRange);
		sortedChangeSupplier.seekToNextChangeInRange(allInclusiveRange);
		sortedChangeSupplier.seekToNextChangeInRange(allInclusiveRange);
		sortedChangeSupplier.seekToNextChangeInRange(allInclusiveRange);
		assertEquals(allGroupingNumbersAfter10BeforeOrAt32, sortedChangeSupplier.findGroupingNumbersForNextChangesBeforeOrAtGroupingNumber(32));
	}
	
	@Test
	public void test_setCursorCheckpoint() throws BlueDbException {
		/*
		 * The batch change application process iterates through the changes more like this. It does one segment, then
		 * moves to the next segment. However, the next segment might contain changes from previous segments that overlap
		 * into this segment. So it ends up processing changes that overlap multiple segments multiple times. Setting a
		 * checkpoint allows us to not have to iterate through changes that we know we never have to look at again.
		 */
		
		Range range = new Range(0, 9);
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change1_1To50, sortedChangeSupplier.getNextChange().orElse(null));
		sortedChangeSupplier.setCursorCheckpoint();
		assertFalse(sortedChangeSupplier.seekToNextChangeInRange(range));
		
		range = new Range(10, 19);
		sortedChangeSupplier.setCursorToLastCheckpoint();
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change1_1To50, sortedChangeSupplier.getNextChange().orElse(null));
		sortedChangeSupplier.setCursorCheckpoint();
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change2_10to60, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change3_10To50, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change4_15_to75, sortedChangeSupplier.getNextChange().orElse(null));
		assertFalse(sortedChangeSupplier.seekToNextChangeInRange(range));
		
		
		range = new Range(20, 29);
		sortedChangeSupplier.setCursorToLastCheckpoint();
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change1_1To50, sortedChangeSupplier.getNextChange().orElse(null));
		sortedChangeSupplier.setCursorCheckpoint();
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change2_10to60, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change3_10To50, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change4_15_to75, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change5_25to28, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change6_27to110, sortedChangeSupplier.getNextChange().orElse(null));
		assertFalse(sortedChangeSupplier.seekToNextChangeInRange(range));
		
		
		range = new Range(30, 39);
		sortedChangeSupplier.setCursorToLastCheckpoint();
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change1_1To50, sortedChangeSupplier.getNextChange().orElse(null));
		sortedChangeSupplier.setCursorCheckpoint();
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change2_10to60, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change3_10To50, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change4_15_to75, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change6_27to110, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change7_32_to_90, sortedChangeSupplier.getNextChange().orElse(null));
		assertFalse(sortedChangeSupplier.seekToNextChangeInRange(range));
		
		
		range = new Range(40, 49);
		sortedChangeSupplier.setCursorToLastCheckpoint();
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change1_1To50, sortedChangeSupplier.getNextChange().orElse(null));
		sortedChangeSupplier.setCursorCheckpoint();
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change2_10to60, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change3_10To50, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change4_15_to75, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change6_27to110, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change7_32_to_90, sortedChangeSupplier.getNextChange().orElse(null));
		assertFalse(sortedChangeSupplier.seekToNextChangeInRange(range));
		
		range = new Range(50, 59);
		sortedChangeSupplier.setCursorToLastCheckpoint();
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change1_1To50, sortedChangeSupplier.getNextChange().orElse(null));
		sortedChangeSupplier.setCursorCheckpoint();
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change2_10to60, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change3_10To50, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change4_15_to75, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change6_27to110, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change7_32_to_90, sortedChangeSupplier.getNextChange().orElse(null));
		assertFalse(sortedChangeSupplier.seekToNextChangeInRange(range));
		
		range = new Range(60, 69);
		sortedChangeSupplier.setCursorToLastCheckpoint();
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change2_10to60, sortedChangeSupplier.getNextChange().orElse(null));
		sortedChangeSupplier.setCursorCheckpoint();
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change4_15_to75, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change6_27to110, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change7_32_to_90, sortedChangeSupplier.getNextChange().orElse(null));
		assertFalse(sortedChangeSupplier.seekToNextChangeInRange(range));
		
		range = new Range(70, 79);
		sortedChangeSupplier.setCursorToLastCheckpoint();
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change4_15_to75, sortedChangeSupplier.getNextChange().orElse(null));
		sortedChangeSupplier.setCursorCheckpoint();
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change6_27to110, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change7_32_to_90, sortedChangeSupplier.getNextChange().orElse(null));
		assertFalse(sortedChangeSupplier.seekToNextChangeInRange(range));
		
		range = new Range(80, 89);
		sortedChangeSupplier.setCursorToLastCheckpoint();
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change6_27to110, sortedChangeSupplier.getNextChange().orElse(null));
		sortedChangeSupplier.setCursorCheckpoint();
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change7_32_to_90, sortedChangeSupplier.getNextChange().orElse(null));
		assertFalse(sortedChangeSupplier.seekToNextChangeInRange(range));
		
		range = new Range(100, 109);
		sortedChangeSupplier.setCursorToLastCheckpoint();
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(range));
		assertEquals(change6_27to110, sortedChangeSupplier.getNextChange().orElse(null));
		sortedChangeSupplier.setCursorCheckpoint();
		assertFalse(sortedChangeSupplier.seekToNextChangeInRange(range));
		
		range = new Range(120, 129);
		assertFalse(sortedChangeSupplier.seekToNextChangeInRange(range));
	}
	
	@Test
	public void test_setCursorToBeginning() throws BlueDbException {
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(new Range(150, 160)));
		assertEquals(change8_150to165, sortedChangeSupplier.getNextChange().orElse(null));
		sortedChangeSupplier.setCursorToBeginning();
		assertEquals(null, sortedChangeSupplier.getNextChange().orElse(null));
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(new Range(0, 10)));
		assertEquals(change1_1To50, sortedChangeSupplier.getNextChange().orElse(null));
	}
	
	@Test
	public void test_nextRangeOverlaps() throws BlueDbException {
		assertFalse(sortedChangeSupplier.nextChangeOverlapsRange(new Range(0, 10)));
		
		assertTrue(sortedChangeSupplier.seekToNextChangeInRange(new Range(80, 90)));
		assertEquals(change6_27to110, sortedChangeSupplier.getNextChange().orElse(null));
		
		assertFalse(sortedChangeSupplier.nextChangeOverlapsRange(new Range(0, 10)));
		assertTrue(sortedChangeSupplier.nextChangeOverlapsRange(new Range(Long.MIN_VALUE, Long.MAX_VALUE)));
		assertTrue(sortedChangeSupplier.nextChangeOverlapsRange(new Range(0, 27)));
		assertTrue(sortedChangeSupplier.nextChangeOverlapsRange(new Range(0, 80)));
		assertTrue(sortedChangeSupplier.nextChangeOverlapsRange(new Range(80, 90)));
		assertTrue(sortedChangeSupplier.nextChangeOverlapsRange(new Range(100, 120)));
		assertFalse(sortedChangeSupplier.nextChangeOverlapsRange(new Range(111, Long.MAX_VALUE)));
	}
}
