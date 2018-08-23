package io.bluedb.disk.collection;

import org.junit.Test;
import io.bluedb.api.Updater;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.collection.task.DeleteMultipleTask;
import io.bluedb.disk.collection.task.DeleteTask;
import io.bluedb.disk.collection.task.InsertTask;
import io.bluedb.disk.collection.task.UpdateMultipleTask;
import io.bluedb.disk.collection.task.UpdateTask;
import io.bluedb.disk.query.BlueQueryOnDisk;
import junit.framework.TestCase;

public class TaskTest extends TestCase {

	@Test
	public void test_DeleteMultipleTask_toString() {
		long min = 37;
		long max = 101;
		BlueQueryOnDisk<?> query = new BlueQueryOnDisk<TestValue>(null);
		query.afterOrAtTime(min).beforeOrAtTime(max);		
		@SuppressWarnings({"rawtypes", "unchecked"})
		Runnable task = new DeleteMultipleTask(null, query);
		String taskString = task.toString();
		assertTrue(taskString.contains(task.getClass().getSimpleName()));
		assertTrue(taskString.contains(String.valueOf(min)));
		assertTrue(taskString.contains(String.valueOf(max)));
	}

	@Test
	public void test_DeleteTask_toString() {
		BlueKey key = new TimeKey(24, 42);
		@SuppressWarnings({"rawtypes", "unchecked"})
		Runnable task = new DeleteTask(null, key);
		String taskString = task.toString();
		assertTrue(taskString.contains(task.getClass().getSimpleName()));
		assertTrue(taskString.contains(key.toString()));
	}

	@Test
	public void test_UpdateMultipleTask_toString() {
		long min = 37;
		long max = 101;
		BlueQueryOnDisk<?> query = new BlueQueryOnDisk<TestValue>(null);
		query.afterOrAtTime(min).beforeOrAtTime(max);		
		@SuppressWarnings({"rawtypes", "unchecked"})
		Runnable task = new UpdateMultipleTask(null, query, null);
		String taskString = task.toString();
		assertTrue(taskString.contains(task.getClass().getSimpleName()));
		assertTrue(taskString.contains(String.valueOf(min)));
		assertTrue(taskString.contains(String.valueOf(max)));
	}

	@Test
	public void test_UpdateTask_toString() {
		BlueKey key = new TimeKey(24, 42);
		Updater<TestValue> updater = ((v) -> v.addCupcake());
		@SuppressWarnings({"rawtypes", "unchecked"})
		Runnable task = new UpdateTask(null, key, updater);
		String taskString = task.toString();
		assertTrue(taskString.contains(task.getClass().getSimpleName()));
		assertTrue(taskString.contains(key.toString()));
	}

	@Test
	public void test_InsertTask_toString() {
		BlueKey key = new TimeKey(24, 42);
		TestValue value = new TestValue("john doe");
		@SuppressWarnings({"rawtypes", "unchecked"})
		Runnable task = new InsertTask(null, key, value);
		String taskString = task.toString();
		assertTrue(taskString.contains(task.getClass().getSimpleName()));
		assertTrue(taskString.contains(key.toString()));
		assertTrue(taskString.contains(value.toString()));
	}
}
