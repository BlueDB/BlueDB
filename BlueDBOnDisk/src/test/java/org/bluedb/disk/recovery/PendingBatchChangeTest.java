package org.bluedb.disk.recovery;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.bluedb.disk.TestValue;

public class PendingBatchChangeTest {

	private static List<IndividualChange<TestValue>> empty = Arrays.asList();

	@Test
	public void test_toString() {
		PendingBatchChange<TestValue> batch = PendingBatchChange.createBatchChange(empty);
		assertTrue(batch.toString().contains(batch.getClass().getSimpleName()));
	}
}
