package io.bluedb.disk.lock;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.StampedLock;

public class LockManager <T> {

	private final Map<T, StampedLock> locks;

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
		StampedLock lock;
		while (!success) {
			synchronized(locks) {
				if (!locks.containsKey(key)) {
					locks.put(key, new StampedLock());
				}
				lock = locks.get(key);
				success = lock.asReadLock().tryLock();
			}
			if (!success) {
				waitForLock(lock);
			}
		}
		return new BlueReadLock<T>(this, key);
	}

	public void releaseReadLock(T key) {
		StampedLock lock = null;
		synchronized(locks) {
			lock = locks.get(key);
			lock.asReadLock().unlock();
			boolean noOtherThreadsAreReading = lock.getReadLockCount() == 0;
			if (noOtherThreadsAreReading) {
				locks.remove(key);
			}
		}
	}

	public BlueWriteLock<T> acquireWriteLock(T key) {
		StampedLock myLock = new StampedLock();
		myLock.asWriteLock().lock();
		StampedLock lockInMap = tryInsertMyLock(key, myLock);
		while (lockInMap != myLock){
			waitForLock(lockInMap);
			lockInMap = tryInsertMyLock(key, myLock);
		}
		return new BlueWriteLock<T>(this, key);
	}

	public void releaseWriteLock(T key) {
		StampedLock lock;
		synchronized(locks) {
			lock = locks.remove(key);
		}
		lock.asWriteLock().unlock();
	}

	// Returns (a) myLock if insert is successful or (b) another thread's lock if another thread is reading or writing
	private StampedLock tryInsertMyLock(T key, StampedLock myLock) {
		synchronized(locks) {
			StampedLock lock = locks.get(key);
			if (lock != null) {
				return lock;
			} else {
				locks.put(key,  myLock);
				return myLock;
			}
		}
	}

	private void waitForLock(StampedLock lock) {
		lock.asWriteLock().lock();  // We know that there's no readers or writers when we can get the writeLock
		// Note that we can't just re-use this lock.  It may have already been removed from the map.
		// We need a lock that we know is in the map and that we got the lock before another thread.
		lock.asWriteLock().unlock();
	}
}
