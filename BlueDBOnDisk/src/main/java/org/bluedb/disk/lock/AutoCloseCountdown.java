package org.bluedb.disk.lock;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.bluedb.disk.Blutils;
import org.bluedb.disk.executors.NamedThreadFactory;

public class AutoCloseCountdown {
	private final static ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, new NamedThreadFactory("bluedb-auto-closer"));
	
	private Closeable target;
	private final long startTime;
	private final AtomicLong expiration;
	private final long duration;
	private final String constructionStackTrace;
	
	private ScheduledFuture<?> autoCloseTask;
	
	private int runCount = 1;

	public AutoCloseCountdown(Closeable closeable, long countdownDuration) {
		if (closeable == null) {
			throw new IllegalArgumentException("Cannot open a " + this.getClass().getSimpleName() + " with a null Closeable.");
		}
		target = closeable;
		duration = countdownDuration;
		startTime = System.currentTimeMillis();
		expiration = new AtomicLong(startTime + duration);
		constructionStackTrace = Blutils.getStackTraceAsString();
		
		if(countdownDuration <= 0) {
			onExpiration();
		} else {
			long delay = Math.max(10, countdownDuration / 3);
			autoCloseTask = executor.scheduleWithFixedDelay(Blutils.surroundTaskWithTryCatch(this::onExpiration), delay, delay, TimeUnit.MILLISECONDS);
		}
	}
	
	public void snooze() {
		long now = System.currentTimeMillis();
		expiration.set(now + duration);
	}

	public void cancel() {
		if(autoCloseTask != null) {
			autoCloseTask.cancel(false);
		}
	}

	public long remainingTime() {
		long now = System.currentTimeMillis();
		long remainingTime = expiration.get() - now;
		remainingTime = Math.max(remainingTime, 0);
		return remainingTime;
	}

	private void onExpiration() {
		long now = System.currentTimeMillis();
		float totalActiveTimeInSeconds = (now - startTime) * .001f;
		
		if (now >= expiration.get()) {
			Blutils.log("[BlueDb Warning] - Auto closing a BlueDb iterator that has been active for " + totalActiveTimeInSeconds + " seconds. Remember to suround BlueDb iterators in a try with resource to ensure that they are closed when you are done. Query Stack Trace:" + System.lineSeparator() + constructionStackTrace);
			closeTarget();
			cancel();
		} else if(runCount % 10 == 0) {
			Blutils.log("[BlueDb Warning] - A BlueDb iterator has been active for " + totalActiveTimeInSeconds + " seconds.  Remember to suround BlueDb iterators in a try with resource to ensure that they are closed when you are done. Query Stack Trace:" + System.lineSeparator() + constructionStackTrace);
		}
		
		runCount++;
	}

	private void closeTarget() {
		try {
			target.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}