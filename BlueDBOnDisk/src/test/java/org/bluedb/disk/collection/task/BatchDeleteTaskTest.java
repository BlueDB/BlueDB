package org.bluedb.disk.collection.task;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.junit.Test;

import org.bluedb.disk.TestValue;

public class BatchDeleteTaskTest {

	@Test
	public void test_toString() {
		BatchChangeTask<TestValue> task = new BatchChangeTask<>(null, new HashMap<>());
		assertTrue(task.toString().contains(task.getClass().getSimpleName()));
	}

}
