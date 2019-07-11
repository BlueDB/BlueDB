package org.bluedb.disk.collection.task;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.serialization.BlueEntity;
import org.junit.Test;

public class DeleteMultipleTaskTest {

	@Test
	public void testCreateSortedChangeList() {
		BlueEntity<TestValue> first = new BlueEntity<>(new TimeKey("a", 1), new TestValue("A"));
		BlueEntity<TestValue> second = new BlueEntity<>(new TimeKey("b", 2), new TestValue("B"));
		BlueEntity<TestValue> third = new BlueEntity<>(new TimeKey("c", 3), new TestValue("C"));
		List<BlueEntity<TestValue>> entities = Arrays.asList(second, first, third);
		List<IndividualChange<TestValue>> changes = DeleteMultipleTask.createSortedChangeList(entities);
		assertEquals(first.getKey(), changes.get(0).getKey());
		assertEquals(second.getKey(), changes.get(1).getKey());
		assertEquals(third.getKey(), changes.get(2).getKey());
	}

}
