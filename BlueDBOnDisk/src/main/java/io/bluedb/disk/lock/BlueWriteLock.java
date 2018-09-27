package io.bluedb.disk.lock;

import java.io.Closeable;

public class BlueWriteLock<T> implements Closeable {

	private final LockManager<T> lockManager;
	private final T key;
	private boolean released = false;

	public BlueWriteLock(LockManager<T> lockManager, T key) {
		this.lockManager = lockManager;
		this.key = key;
	}

	public T getKey() {
		return key;
	}

	public void release() {
		if (!released) {
			lockManager.releaseWriteLock(key);
		}
		released = true;
	}

	@Override
	public void close() {
		release();
	}
}
