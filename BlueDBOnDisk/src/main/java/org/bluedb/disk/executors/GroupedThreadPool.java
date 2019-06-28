package org.bluedb.disk.executors;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Wraps a thread pool that groups tasks based on a group id. Only one task will be allowed to run at a time for each group id.
 */
public class GroupedThreadPool {
	private final ThreadPoolExecutor executor;
	private final Map<String, List<FutureTask<Void>>> tasksByGroupId = new HashMap<>();
	private boolean isShutdown = false;
	
	private final Object lock = "GroupedThreadPool Lock";
	
	public GroupedThreadPool(ThreadPoolExecutor executor) {
		this.executor = executor;
	}
	
	public static GroupedThreadPool createCachedPool(String threadPrefix) {
		return new GroupedThreadPool((ThreadPoolExecutor) Executors.newCachedThreadPool(new NamedThreadFactory(threadPrefix)));
	}
	
	public Future<?> submit(String groupId, Runnable runnable) {
		FutureTask<Void> future = new FutureTask<>(runnable, null);
		
		synchronized (lock) {
			if(isShutdown) {
				throw new RejectedExecutionException();
			}
			
			List<FutureTask<Void>> groupedTasks = tasksByGroupId.get(groupId);
			if (groupedTasks == null) {
				groupedTasks = new LinkedList<>();
				tasksByGroupId.put(groupId, groupedTasks);
				groupedTasks.add(future);
				submitNextTaskForGroup(groupId);		
			}
			else {
				groupedTasks.add(future);
			}
			return future;
		}
	}

	private void submitNextTaskForGroup(String groupId) {
		synchronized (lock) {
			List<FutureTask<Void>> groupedTasks = tasksByGroupId.get(groupId);
			
			if (groupedTasks == null || groupedTasks.isEmpty()) {
				tasksByGroupId.remove(groupId);
				return;
			}
			
			FutureTask<Void> task = groupedTasks.remove(0);
			executor.submit(new GroupTask(groupId, task));
		}
	}
	
	public int getQueueSizeForGroup(String groupId) {
		synchronized (lock) {
			if(isShutdown) {
				return 0;
			}
			
			List<FutureTask<Void>> groupedTasks = tasksByGroupId.get(groupId);
			return groupedTasks != null ? groupedTasks.size() : 0;
		}
	}
	
	public int getActiveCount() {
		return executor.getActiveCount();
	}
	
	public int getLargestPoolSize() {
		return executor.getLargestPoolSize();
	}

	public void shutdown() {
		synchronized (lock) {
			isShutdown = true;
			tasksByGroupId.clear();
			executor.shutdown();
		}
	}

	public void shutdownNow() {
		synchronized (lock) {
			isShutdown = true;
			tasksByGroupId.clear();
			executor.shutdownNow();
		}
	}

	public boolean awaitTermination(long timeout, TimeUnit timeUnit) throws InterruptedException {
		return executor.awaitTermination(timeout, timeUnit);
	}

	private class GroupTask implements Runnable {
		private String groupId;
		private FutureTask<Void> futureTask;
		
		public GroupTask(String groupId, FutureTask<Void> futureTask) {
			this.groupId = groupId;
			this.futureTask = futureTask;
		}
	
		@Override
		public void run() {
			futureTask.run();
			handleAnyThrownErrors();
			submitNextTaskForGroup(groupId);
		}
	
		private void handleAnyThrownErrors() {
			try {
				futureTask.get();
			} catch(Throwable t) {
				t.printStackTrace();
			}
		}
	}
}
