package org.bluedb.tasks;

import java.util.concurrent.CountDownLatch;

public class TestTask implements Runnable {
	private ThrowingRunnable runnable;
	private Throwable error;
	
	private CountDownLatch startLatch = new CountDownLatch(1);
	private CountDownLatch completionLatch = new CountDownLatch(1);

	public TestTask(ThrowingRunnable runnable) {
		this.runnable = runnable;
	}

	@Override
	public void run() {
		startLatch.countDown();
		
		try {
			runnable.run();
		} catch(Throwable t) {
			error = t;
		} finally {
			completionLatch.countDown();
		}
	}
	
	public void awaitStart() throws InterruptedException {
		startLatch.await();
	}
	
	public void awaitCompletion() throws InterruptedException {
		completionLatch.await();
	}
	
	public boolean isComplete() {
		return completionLatch.getCount() == 0;
	}
	
	public Throwable getError() {
		return error;
	}
	
	public static TestTask run(ThrowingRunnable runnable) {
		TestTask task = new TestTask(runnable);
		task.run();
		return task;
	}
}
