package org.bluedb.disk.recovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.segment.Range;
import org.junit.Test;

public class PendingBatchChangeTest {

	private static List<IndividualChange<TestValue>> empty = Arrays.asList();

	@Test
	public void test_toString() {
		PendingBatchChange<TestValue> batch = PendingBatchChange.createBatchChange(empty);
		assertTrue(batch.toString().contains(batch.getClass().getSimpleName()));
	}
	
	@Test
	public void test_nullOrEmptyInConstructor() {
		PendingBatchChange<TestValue> batchChange = PendingBatchChange.createBatchChange(null);
		assertEquals(empty, batchChange.getSortedChanges());
		
		batchChange = PendingBatchChange.createBatchChange(empty);
		assertEquals(empty, batchChange.getSortedChanges());
	}
	
	@Test
	public void test_removeChangesOutsideRange() {
		IndividualChange<TestValue> excludedChange1 = IndividualChange.manuallyCreateTestChange(new TimeKey(1, 1), null, new TestValue("Alfred", 1), Optional.empty());
		IndividualChange<TestValue> excludedChange2 = IndividualChange.manuallyCreateTestChange(new TimeKey(2, 2), new TestValue("Bob", 2), new TestValue("Bob", 3), Optional.empty());
		IndividualChange<TestValue> excludedChange3 = IndividualChange.manuallyCreateTestChange(new TimeKey(3, 3), new TestValue("Charlie", 3), null, Optional.empty());

		IndividualChange<TestValue> includedChange1 = IndividualChange.manuallyCreateTestChange(new TimeKey(4, 4), null, new TestValue("Ed", 4), Optional.empty());
		IndividualChange<TestValue> includedChange2 = IndividualChange.manuallyCreateTestChange(new TimeKey(5, 5), new TestValue("Frank", 5), new TestValue("Bob", 6), Optional.empty());
		IndividualChange<TestValue> includedChange3 = IndividualChange.manuallyCreateTestChange(new TimeKey(6, 6), new TestValue("George", 6), null, Optional.empty());
		
		IndividualChange<TestValue> excludedChange4 = IndividualChange.manuallyCreateTestChange(new TimeKey(7, 7), null, new TestValue("Alfred", 7), Optional.empty());
		IndividualChange<TestValue> excludedChange5 = IndividualChange.manuallyCreateTestChange(new TimeKey(8, 8), new TestValue("Bob", 8), new TestValue("Bob", 9), Optional.empty());
		IndividualChange<TestValue> excludedChange6 = IndividualChange.manuallyCreateTestChange(new TimeKey(9, 9), new TestValue("Charlie", 9), null, Optional.empty());
		
		List<IndividualChange<TestValue>> allChanges = Arrays.asList(excludedChange1, excludedChange2, excludedChange3,
				includedChange1, includedChange2, includedChange3, excludedChange4, excludedChange5, excludedChange6);
		
		List<IndividualChange<TestValue>> includedChanges = Arrays.asList(includedChange1, includedChange2, includedChange3);
		
		PendingBatchChange<TestValue> batchChange = PendingBatchChange.createBatchChange(allChanges);
		
		assertEquals(allChanges, batchChange.getSortedChanges());
		batchChange.removeChangesOutsideRange(new Range(4, 6));
		assertEquals(includedChanges, batchChange.getSortedChanges());
	}
}
