package org.bluedb.disk.lock;

import java.io.Closeable;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

public class AutoCloseCountdown {

	private Closeable target;
	private Timer timer;
	private final AtomicLong expiration;
	private final long duration;

	public AutoCloseCountdown(Closeable closeable, long countdownDuration) {
		if (closeable == null) {
			throw new IllegalArgumentException("Cannot open a " + this.getClass().getSimpleName() + " with a null Closeable.");
		}
		target = closeable;
		duration = countdownDuration;
		expiration = new AtomicLong(System.currentTimeMillis() + duration);
		startCountdown();
	}
	
	public void snooze() {
		long now = System.currentTimeMillis();
		expiration.set(now + duration);
	}

	public void cancel() {
		clearTimer();
	}

	public long remainingTime() {
		long now = System.currentTimeMillis();
		long remainingTime = expiration.get() - now;
		remainingTime = Math.max(remainingTime, 0);
		return remainingTime;
	}

	private void clearTimer() {
		if (timer != null) {
			timer.cancel();
			timer = null;
		}
	}

	private void onExpiration() {
		long now = System.currentTimeMillis();
		if (now >= expiration.get()) {
			closeTarget();
		} else {
			startCountdown();
		}
	}

	private void closeTarget() {
		try {
			target.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void startCountdown() {
		clearTimer();
		TimerTask newExpirationTask = new TimerTask() {
			@Override public void run() {
				onExpiration();
			}
		};
		long remainingTime = remainingTime();
		if (remainingTime == 0) {
			closeTarget();
		} else {
			timer = new Timer("BlueDB-Query-Iterator-Timeout-Thread", true);
			timer.schedule(newExpirationTask, remainingTime);
		}
	}
}