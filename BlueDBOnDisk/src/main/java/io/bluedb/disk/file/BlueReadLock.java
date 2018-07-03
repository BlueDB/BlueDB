package io.bluedb.disk.file;

import java.io.Closeable;

public class BlueReadLock<T> implements Closeable {

	private final LockManager<T> lockManager;
	private final T key;
	private boolean released = false;

	public BlueReadLock(LockManager<T> lockManager, T key) {
		this.lockManager = lockManager;
		this.key = key;
	}

	public T getKey() {
		return key;
	}

	public void release() {
		if (!released) {
			lockManager.releaseReadLock(key);
		}
		released = true;
	}

	@Override
	public void close() {
		release();
	}
}
