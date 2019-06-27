package org.bluedb.disk.executors;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class BlueExecutor {
	private final GroupedThreadPool queryTaskExecutor;
	private final ScheduledThreadPoolExecutor scheduledTaskExecutor;

	public BlueExecutor(String name) {
		queryTaskExecutor = GroupedThreadPool.createCachedPool(name + "-query-task-executor");
		scheduledTaskExecutor = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory(name + "-scheduled-task-executor"));
	}

	public Future<?> submitQueryTask(String collectionName, Runnable task) {
		Future<?> future = queryTaskExecutor.submit(collectionName, task);
		return future;
	}
	
	public int getQueryQueueSize(String collectionName) {
		return queryTaskExecutor.getQueueSizeForGroup(collectionName);
	}

	public void scheduleTaskAtFixedRate(Runnable task, long initialDelay, long period, TimeUnit timeUnit) {
		scheduledTaskExecutor.scheduleAtFixedRate(task, initialDelay, period, timeUnit);
	}

	public void shutdown() {
		queryTaskExecutor.shutdown();
		scheduledTaskExecutor.shutdown();
	}

	public void shutdownNow() {
		queryTaskExecutor.shutdownNow();
		scheduledTaskExecutor.shutdownNow();
	}
	
	public boolean awaitTermination(long timeout, TimeUnit timeUnit) throws InterruptedException {
		boolean queryExecutorTerminated = queryTaskExecutor.awaitTermination(timeout, timeUnit);
		boolean scheduledExecutorTerminated = scheduledTaskExecutor.awaitTermination(timeout, timeUnit);
		return queryExecutorTerminated && scheduledExecutorTerminated;
	}
}
