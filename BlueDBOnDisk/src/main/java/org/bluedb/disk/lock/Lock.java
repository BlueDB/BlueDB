package org.bluedb.disk.lock;

import java.util.concurrent.locks.StampedLock;

public class Lock<T> {
//	private static final AtomicLong NEXT_ID = new AtomicLong(0);
	
//	private final long id;
	private final StampedLock lock;
//	private final T lockedKey;
	
	public Lock(T lockedKey) {
//		this.id = NEXT_ID.getAndIncrement();
		this.lock = new StampedLock();
//		this.lockedKey = lockedKey;
	}

	public boolean tryToLockForRead() {
		boolean success = lock.asReadLock().tryLock();
//		printLockDebugMessage((success ? "Got" : "Failed to get") + " read lock", UUID.randomUUID());
		return success;
	}

	public void unlockForRead() {
//		UUID sessionId = UUID.randomUUID();
//		printLockDebugMessage("Unlocking read lock", sessionId);
		lock.asReadLock().unlock();
//		printLockDebugMessage("Unlocked read lock", sessionId);
	}

	public boolean isLockedForRead() {
		return lock.getReadLockCount() > 0;
	}

	public void lockForWrite() {
//		UUID sessionId = UUID.randomUUID();
//		printLockDebugMessage("Locking write lock", sessionId);
		lock.asWriteLock().lock();
//		printLockDebugMessage("Locked write lock", sessionId);
	}

	public void unlockForWrite() {
//		UUID sessionId = UUID.randomUUID();
//		printLockDebugMessage("Unlocking write lock", sessionId);
		lock.asWriteLock().unlock();
//		printLockDebugMessage("Unlocked write lock", sessionId);
	}

	public void lockAndUnlockForWrite() {
		lockForWrite();// We know that there's no readers or writers when we can get the writeLock
		// Note that we can't just re-use this lock.  It may have already been removed from the map.
		// We need a lock that we know is in the map and that we got the lock before another thread.
		unlockForWrite();
	}

//	private void printLockDebugMessage(String message, UUID sessionId) {
//		String output = "[BlueDbLockDebug] - " + message + " Key: " + lockedKey + "(" + id + ")" + " Thread: " + Thread.currentThread() + " Session: " + sessionId;
//		Blutils.log(output + System.lineSeparator() + Blutils.getStackTraceAsString());
//	}
}
