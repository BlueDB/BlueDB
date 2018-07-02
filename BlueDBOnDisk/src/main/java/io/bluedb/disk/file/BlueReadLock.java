package io.bluedb.disk.file;

public class BlueReadLock<T> {

	private final LockManager<T> lockManager;
	private final T key;

	public BlueReadLock(LockManager<T> lockManager, T key) {
		this.lockManager = lockManager;
		this.key = key;
	}

	public T getKey() {
		return key;
	}

	public void release() {
		lockManager.releaseReadLock(key);
	}
}
