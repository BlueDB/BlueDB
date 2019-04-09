package io.bluedb.disk.recovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.TestValue;

public class PendingBatchChangeTest {
	private static BlueKey key0At0 = new TimeKey(0, 0);
	private static BlueKey key1At1To5 = new TimeFrameKey(1, 1, 5);
	private static BlueKey key4At4 = new TimeKey(4, 4);
	private static TestValue value0 = new TestValue("0");
	private static TestValue value1 = new TestValue("1");
	private static TestValue value4 = new TestValue("4");
	private static IndividualChange<TestValue> insert0At0 = IndividualChange.insert(key0At0, value0);
	private static IndividualChange<TestValue> insert1At1To5 = IndividualChange.insert(key1At1To5, value1);
	private static IndividualChange<TestValue> insert4At4 = IndividualChange.insert(key4At4, value4);
	private static List<IndividualChange<TestValue>> empty = Arrays.asList();
	private static List<IndividualChange<TestValue>> inserts0 = Arrays.asList(insert0At0);
	private static List<IndividualChange<TestValue>> inserts0and1to5 = Arrays.asList(insert0At0, insert1At1To5);
	private static List<IndividualChange<TestValue>> inserts0and1to5and4 = Arrays.asList(insert0At0, insert1At1To5, insert4At4);
	private static List<IndividualChange<TestValue>> inserts1to5and4 = Arrays.asList(insert1At1To5, insert4At4);
	private static List<IndividualChange<TestValue>> inserts1to5 = Arrays.asList(insert1At1To5);

	@Test
	public void test_removeChangesEndingBefore() {
		LinkedList<IndividualChange<TestValue>> list;

		list = new LinkedList<>(inserts0and1to5and4); 
		PendingBatchChange.removeChangesEndingBeforeOrAt(list, -1);
		assertEquals(inserts0and1to5and4, list);

		list = new LinkedList<>(inserts0and1to5and4); 
		PendingBatchChange.removeChangesEndingBeforeOrAt(list, 0);
		assertEquals(inserts1to5and4, list);

		list = new LinkedList<>(inserts0and1to5and4); 
		PendingBatchChange.removeChangesEndingBeforeOrAt(list, 1);
		assertEquals(inserts1to5and4, list);

		list = new LinkedList<>(inserts0and1to5and4); 
		PendingBatchChange.removeChangesEndingBeforeOrAt(list, 4);
		assertEquals(inserts1to5, list);

		list = new LinkedList<>(inserts0and1to5and4); 
		PendingBatchChange.removeChangesEndingBeforeOrAt(list, 5);
		assertEquals(empty, list);
	}

	@Test
	public void test_getChangesBeforeOrAt() {
		LinkedList<IndividualChange<TestValue>> result;

		result = PendingBatchChange.getChangesBeforeOrAt(inserts0and1to5and4, -1);
		assertEquals(empty, result);

		result = PendingBatchChange.getChangesBeforeOrAt(inserts0and1to5and4, 0);
		assertEquals(inserts0, result);

		result = PendingBatchChange.getChangesBeforeOrAt(inserts0and1to5and4, 1);
		assertEquals(inserts0and1to5, result);

		result = PendingBatchChange.getChangesBeforeOrAt(inserts0and1to5and4, 4);
		assertEquals(inserts0and1to5and4, result);
	}

	@Test
	public void test_toString() {
		PendingBatchChange<TestValue> batch = PendingBatchChange.createBatchUpsert(empty);
		assertTrue(batch.toString().contains(batch.getClass().getSimpleName()));
	}
}
