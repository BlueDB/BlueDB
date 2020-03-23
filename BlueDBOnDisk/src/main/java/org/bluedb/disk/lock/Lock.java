package org.bluedb.disk.lock;

import java.util.concurrent.locks.StampedLock;

public class Lock<T> {
	private T key;
	private StampedLock lock;
	private String threadName;
	
	public Lock(T key) {
		this.key = key;
		this.lock = new StampedLock();
		this.threadName = Thread.currentThread().getName();
	}

	public boolean tryToLockForRead() {
		return lock.asReadLock().tryLock();
	}

	public void unlockForRead() {
		lock.asReadLock().unlock();
	}

	public boolean isLockedForRead() {
		return lock.getReadLockCount() > 0;
	}

	public void lockForWrite() {
		lock.asWriteLock().lock();
	}

	public void unlockForWrite() {
		lock.asWriteLock().unlock();
	}

	public void lockAndUnlockForWrite() {
		lockForWrite();// We know that there's no readers or writers when we can get the writeLock
		// Note that we can't just re-use this lock.  It may have already been removed from the map.
		// We need a lock that we know is in the map and that we got the lock before another thread.
		unlockForWrite();
	}
}
