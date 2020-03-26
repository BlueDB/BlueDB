package org.bluedb.disk.lock;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

public class BlueReadLock<T> implements Closeable {

	private final LockManager<T> lockManager;
	private final T key;
	private AtomicBoolean released = new AtomicBoolean(false);

	public BlueReadLock(LockManager<T> lockManager, T key) {
		this.lockManager = lockManager;
		this.key = key;
	}

	public T getKey() {
		return key;
	}

	public void release() {
		if (!released.getAndSet(true)) {
			lockManager.releaseReadLock(key);
		}
	}

	@Override
	public void close() {
		release();
	}
}
