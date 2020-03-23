package org.bluedb.disk.lock;

import java.util.HashMap;
import java.util.Map;

public class LockManager <T> {

	private final Map<T, StampedLock> locks;
	
	private final Map<T, Lock<T>> locks;

	public LockManager() {
		locks = new HashMap<>();
	}

	public boolean isLocked(T key) {
		synchronized(locks) {
			return locks.containsKey(key);
		}
	}

	public BlueReadLock<T> acquireReadLock(T key) {
		boolean success = false;
		Lock<T> lock;
		while (!success) {
			synchronized(locks) {
				lock = locks.get(key);
				if(lock == null) {
					lock = new Lock<>(key);
					locks.put(key, lock);
				}
				success = lock.tryToLockForRead();
			}
			if (!success) {
				lock.lockAndUnlockForWrite();
			}
		}
		return new BlueReadLock<T>(this, key);
	}

	public void releaseReadLock(T key) {
		Lock<T> lock = null;
		synchronized(locks) {
			lock = locks.get(key);
			lock.unlockForRead();
			if (!lock.isLockedForRead()) {
				locks.remove(key);
			}
		}
	}

	public BlueWriteLock<T> acquireWriteLock(T key) {
		Lock<T> myLock = new Lock<>(key);
		myLock.lockForWrite();
		Lock<T> lockInMap = tryInsertMyLock(key, myLock);
		while (lockInMap != myLock){
			lockInMap.lockAndUnlockForWrite();
			lockInMap = tryInsertMyLock(key, myLock);
		}
		return new BlueWriteLock<T>(this, key);
	}

	public void releaseWriteLock(T key) {
		Lock<T> lock;
		synchronized(locks) {
			lock = locks.remove(key);
		}
		lock.unlockForWrite();
	}

	// Returns (a) myLock if insert is successful or (b) another thread's lock if another thread is reading or writing
	private Lock<T> tryInsertMyLock(T key, Lock<T> myLock) {
		synchronized(locks) {
			Lock<T> lock = locks.get(key);
			if (lock != null) {
				return lock;
			} else {
				locks.put(key, myLock);
				return myLock;
			}
		}
	}
}
