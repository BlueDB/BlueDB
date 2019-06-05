package io.bluedb.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;

public class CachedSingleThreadingPool {

	
	private ConcurrentHashMap<String, ConcurrentLinkedQueue<FutureTask<Void>>> collectionTaskMap = new ConcurrentHashMap<>();
	private ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
	
	public Future<?> submit(String collectionName, Runnable runnable) {
		
		synchronized (collectionTaskMap) {
			FutureTask<Void> future = new FutureTask<>(runnable, null);
			
			ConcurrentLinkedQueue<FutureTask<Void>> collectionTaskQueue = collectionTaskMap.get(collectionName);
			if (collectionTaskQueue == null) {
				collectionTaskQueue = new ConcurrentLinkedQueue<>();
				collectionTaskMap.put(collectionName, collectionTaskQueue);
				collectionTaskQueue.add(future);
				pollAndSubmit(collectionName);		
			}
			else {
				collectionTaskQueue.add(future);
			}
				
			
			return future;
		}
	}
	
	

	protected void pollAndSubmit(String collectionName) {
		synchronized (collectionTaskMap) {
			ConcurrentLinkedQueue<FutureTask<Void>> collectionTaskQueue = collectionTaskMap.get(collectionName);
			if (collectionTaskQueue == null || collectionTaskQueue.isEmpty()) {
				collectionTaskMap.remove(collectionName);
				return;
			}
			
			FutureTask<Void> task = collectionTaskQueue.poll();
			cachedThreadPool.submit(runPollSubmit(task, collectionName));
		}
		
	}
	
	
	private Runnable runPollSubmit(FutureTask<Void> futureTask, String collection) {
		return new Runnable() {
			@Override
			public void run() {
				futureTask.run();
				
				
				pollAndSubmit(collection);
			}
		};
	}
	
	public int getActiveCount() {
		return ((ThreadPoolExecutor)cachedThreadPool).getActiveCount();
	}
	
	public int getLargestPoolSize() {
		return ((ThreadPoolExecutor)cachedThreadPool).getLargestPoolSize();
	}

}
