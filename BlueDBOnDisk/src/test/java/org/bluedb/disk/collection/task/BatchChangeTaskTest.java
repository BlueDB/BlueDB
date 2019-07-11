package org.bluedb.disk.collection.task;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.bluedb.disk.TestValue;
import org.junit.Test;

public class BatchChangeTaskTest {

	@Test
	public void test_toString() {
		BatchDeleteTask<TestValue> task = new BatchDeleteTask<TestValue>(null, Arrays.asList());
		assertTrue(task.toString().contains(task.getClass().getSimpleName()));
	}

}
