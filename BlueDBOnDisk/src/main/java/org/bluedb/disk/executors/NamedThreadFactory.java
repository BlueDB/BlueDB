package org.bluedb.disk.executors;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {
	private String threadPrefix;
	private AtomicInteger threadIndex = new AtomicInteger();

	public NamedThreadFactory(String threadPrefix) {
		this.threadPrefix = threadPrefix;
	}
	
	@Override
	public Thread newThread(Runnable r) {
		Thread t = new Thread(r, threadPrefix + threadIndex.getAndIncrement());
		t.setDaemon(true);
		return t;
	}

}
