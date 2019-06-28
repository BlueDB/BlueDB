package org.bluedb.tasks;

public class AsynchronousTestTask {
	private Thread thread;
	private Throwable error;
	private boolean complete = false;

	public AsynchronousTestTask(ThrowingRunnable runnable) {
		this.thread = new Thread(createPureRunnable(runnable));
	}

	private Runnable createPureRunnable(ThrowingRunnable runnable) {
		return () -> {
			try {
				runnable.run();
			} catch(Throwable t) {
				error = t;
			} finally {
				complete = true;
			}
		};
	}
	
	public void run() {
		thread.start();
	}
	
	public void interrupt() {
		thread.interrupt();
	}
	
	public void awaitCompletion() throws InterruptedException {
		thread.join();
	}
	
	public boolean isComplete() {
		return complete;
	}
	
	public Throwable getError() {
		return error;
	}
	
	public static AsynchronousTestTask run(ThrowingRunnable runnable) {
		AsynchronousTestTask task = new AsynchronousTestTask(runnable);
		task.run();
		return task;
	}
}
