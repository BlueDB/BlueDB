package io.bluedb.disk.file;

public class BlueWriteLock<T> {

	private final LockManager<T> lockManager;
	private final T key;

	public BlueWriteLock(LockManager<T> lockManager, T key) {
		this.lockManager = lockManager;
		this.key = key;
	}

	public T getKey() {
		return key;
	}

	public void release() {
		lockManager.releaseWriteLock(key);
	}
}
