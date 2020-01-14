package org.bluedb.disk.collection;

import org.bluedb.api.Updater;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.task.DeleteMultipleTask;
import org.bluedb.disk.collection.task.DeleteTask;
import org.bluedb.disk.collection.task.InsertTask;
import org.bluedb.disk.collection.task.ReplaceMultipleTask;
import org.bluedb.disk.collection.task.ReplaceTask;
import org.bluedb.disk.collection.task.UpdateMultipleTask;
import org.bluedb.disk.collection.task.UpdateTask;
import org.bluedb.disk.query.BlueTimeQueryOnDisk;
import org.junit.Test;

import junit.framework.TestCase;

public class TaskTest extends TestCase {

	@Test
	public void test_DeleteMultipleTask_toString() {
		long min = 37;
		long max = 101;
		BlueTimeQueryOnDisk<?> query = new BlueTimeQueryOnDisk<TestValue>(null);
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
		BlueTimeQueryOnDisk<?> query = new BlueTimeQueryOnDisk<TestValue>(null);
		query.afterOrAtTime(min).beforeOrAtTime(max);		
		@SuppressWarnings({"rawtypes", "unchecked"})
		Runnable task = new UpdateMultipleTask(null, query, null);
		String taskString = task.toString();
		assertTrue(taskString.contains(task.getClass().getSimpleName()));
		assertTrue(taskString.contains(String.valueOf(min)));
		assertTrue(taskString.contains(String.valueOf(max)));
	}

	@Test
	public void test_ReplaceMultipleTask_toString() {
		long min = 37;
		long max = 101;
		BlueTimeQueryOnDisk<?> query = new BlueTimeQueryOnDisk<TestValue>(null);
		query.afterOrAtTime(min).beforeOrAtTime(max);		
		@SuppressWarnings({"rawtypes", "unchecked"})
		Runnable task = new ReplaceMultipleTask(null, query, null);
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
	public void test_ReplaceTask_toString() {
		BlueKey key = new TimeKey(24, 42);
		@SuppressWarnings({"rawtypes", "unchecked"})
		Runnable task = new ReplaceTask(null, key, (v) -> v);
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
