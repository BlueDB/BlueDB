package org.bluedb.disk.collection.task;

import static org.junit.Assert.*;

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
		
		assertEquals(new IndividualChange<>(first.getKey(), first.getValue(), null), DeleteMultipleTask.createChange(first));
		assertEquals(new IndividualChange<>(second.getKey(), second.getValue(), null), DeleteMultipleTask.createChange(second));
		assertEquals(new IndividualChange<>(third.getKey(), third.getValue(), null), DeleteMultipleTask.createChange(third));
	}

}
