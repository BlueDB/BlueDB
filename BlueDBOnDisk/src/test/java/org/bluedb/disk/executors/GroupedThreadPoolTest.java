package org.bluedb.disk.executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.bluedb.TestUtils;
import org.bluedb.tasks.TestTask;
import org.junit.Test;

public class GroupedThreadPoolTest {

	@Test
	public void testSubmit() throws InterruptedException, ExecutionException {
		GroupedThreadPool pool = GroupedThreadPool.createCachedPool("test");
		final StringBuilder angle = new StringBuilder();
		final StringBuilder breads = new StringBuilder();
		final StringBuilder cat = new StringBuilder();
		final StringBuilder dog = new StringBuilder();
		
		pool.submit("A", () -> appendAfterSleep(angle, "a", 50)); pool.submit("C", () -> appendAfterSleep(cat, "a", 50));  
		pool.submit("A", () -> appendAfterSleep(angle, "n", 40));
		pool.submit("A", () -> { throw new NullPointerException(); });
		pool.submit("A", () -> appendAfterSleep(angle, "g", 30)); 
		pool.submit("A", () -> appendAfterSleep(angle, "l", 20)); pool.submit("ZZZ", () -> appendAfterSleep(cat, "c", 0));
		pool.submit("A", () -> appendAfterSleep(angle, "e", 10)); 
		assertEquals(5, pool.getQueueSizeForGroup("A"));
		Future<?> angleTest = pool.submit("A", () -> assertEquals("angle", angle.toString())); 
		
		int activeCount = pool.getActiveCount();
		int largestPoolSize = pool.getLargestPoolSize();
		assertTrue("There should be <= 3 active threads - actual: " + activeCount, activeCount <= 3);
		assertTrue("There should be <= 3 max threads - actual: " + largestPoolSize, largestPoolSize <= 3);
		
		
		pool.submit("B", () -> appendAfterSleep(breads, "b", 50)); //this should use the ZZZ thread
		pool.submit("B", () -> appendAfterSleep(breads, "r", 0)); pool.submit("C", () -> appendAfterSleep(cat, "t", 0));
		pool.submit("B", () -> appendAfterSleep(breads, "e", 40));
		pool.submit("B", () -> appendAfterSleep(breads, "a", 30));
		pool.submit("B", () -> appendAfterSleep(breads, "d", 20)); Future<?> catTest = pool.submit("C", () -> assertEquals("cat", cat.toString()));
		pool.submit("B", () -> appendAfterSleep(breads, "s", 10));
		Future<?> breadsTest = pool.submit("B", () -> assertEquals("breads", breads.toString()));
		
		activeCount = pool.getActiveCount();
		largestPoolSize = pool.getLargestPoolSize();
		assertTrue("There should be <= 3 active threads - actual: " + activeCount, activeCount <= 3);
		assertTrue("There should be <= 4 max threads - actual: " + largestPoolSize, largestPoolSize <= 4);
		
		angleTest.get();
		Thread.sleep(1);
		
		pool.submit("D", () -> dog.append("d")); //should use A thread
		pool.submit("D", () -> dog.append("o"));
		pool.submit("D", () -> dog.append("g"));
		Future<?> dogTest = pool.submit("D", () -> assertEquals("dog", dog.toString()));
		
		activeCount = pool.getActiveCount();
		largestPoolSize = pool.getLargestPoolSize();
		assertTrue("There should be <= 3 active threads - actual: " + activeCount, activeCount <= 3);
		assertTrue("There should be <= 5 max threads - actual: " + largestPoolSize, largestPoolSize <= 5);
		
		angleTest.get();
		breadsTest.get();
		catTest.get();
		dogTest.get();
		
		Thread.sleep(15);
		activeCount = pool.getActiveCount();
		assertTrue("There should be 0 active threads - actual: " + activeCount, activeCount == 0);
		
	}
	
	private void appendAfterSleep(StringBuilder sb, String stringToAppend, long timeout) {
		try {
			if (timeout > 0)
				Thread.sleep(timeout);
			sb.append(stringToAppend);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		
	}
	
	@Test
	public void test_shutdown() throws InterruptedException {
		GroupedThreadPool pool = GroupedThreadPool.createCachedPool("test");
		
		TestTask sleepTask = new TestTask(() -> {
			Thread.sleep(10);
		});
		
		pool.submit("collection-1", sleepTask);
		sleepTask.awaitStart();
		pool.shutdown();
		
		assertEquals(0, pool.getQueueSizeForGroup("collection-1"));
		
		TestTask taskToBeRejected = TestTask.run(() -> {
			pool.submit("collection-2", () -> System.out.println("Collection 2 task"));
		});

		assertEquals(true, pool.awaitTermination(1, TimeUnit.MINUTES));
		
		assertEquals(true, sleepTask.isComplete());
		assertEquals(true, sleepTask.getError() == null);
		
		assertEquals(true, taskToBeRejected.isComplete());
		TestUtils.assertThrowable(RejectedExecutionException.class, taskToBeRejected.getError());
	}
	
	@Test
	public void test_shutdownNow() throws InterruptedException {
		GroupedThreadPool pool = GroupedThreadPool.createCachedPool("test");
		
		TestTask sleepTask = new TestTask(() -> {
			Thread.sleep(10);
		});
		
		pool.submit("collection-1", sleepTask);
		sleepTask.awaitStart();
		pool.shutdownNow();
		
		assertEquals(0, pool.getQueueSizeForGroup("collection-1"));
		
		TestTask taskToBeRejected = TestTask.run(() -> {
			pool.submit("collection-2", () -> System.out.println("Collection 2 task"));
		});

		assertEquals(true, pool.awaitTermination(3, TimeUnit.MILLISECONDS));
		
		assertEquals(true, sleepTask.isComplete());
		TestUtils.assertThrowable(InterruptedException.class, sleepTask.getError());
		
		assertEquals(true, taskToBeRejected.isComplete());
		TestUtils.assertThrowable(RejectedExecutionException.class, taskToBeRejected.getError());
	}
}
