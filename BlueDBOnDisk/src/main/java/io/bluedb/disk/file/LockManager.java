package io.bluedb.disk.file;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockManager <T> {

	private final Map<T, ReentrantReadWriteLock> locks;

	public LockManager() {
		locks = new HashMap<>();
	}

	public void acquireReadLock(T key) {
		boolean success = false;
		ReentrantReadWriteLock lock;
		while (!success) {
			synchronized(locks) {
				if (!locks.containsKey(key)) {
					locks.put(key, new ReentrantReadWriteLock());
				}
				lock = locks.get(key);
				success = lock.readLock().tryLock();
			}
			if (!success) {
				waitForLock(lock);
			}
		}
	}

	public void releaseReadLock(T key) {
		ReentrantReadWriteLock lock = null;
		synchronized(locks) {
			lock = locks.get(key);
			lock.readLock().unlock();
			boolean noOtherThreadsAreReading = lock.writeLock().tryLock();
			if (noOtherThreadsAreReading) {
				lock = locks.remove(key);
				lock.writeLock().unlock();
			}
		}
	}

	public void acquireWriteLock(T key) {
		ReentrantReadWriteLock myLock = new ReentrantReadWriteLock();
		myLock.writeLock().lock();
		ReentrantReadWriteLock lockInMap = tryInsertMyLock(key, myLock);
		while (lockInMap != myLock){
			waitForLock(lockInMap);
			lockInMap = tryInsertMyLock(key, myLock);
		}
	}

	public void releaseWriteLock(T key) {
		ReentrantReadWriteLock lock;
		synchronized(locks) {
			lock = locks.remove(key);
		}
		lock.writeLock().unlock();
	}

	// Returns (a) myLock if insert is successful or (b) another thread's lock if another thread is reading or writing
	private ReentrantReadWriteLock tryInsertMyLock(T key, ReentrantReadWriteLock myLock) {
		synchronized(locks) {
			ReentrantReadWriteLock lock = locks.get(key);
			if (lock != null) {
				return lock;
			} else {
				locks.put(key,  myLock);
				return myLock;
			}
		}
	}

	private void waitForLock(ReentrantReadWriteLock lock) {
		lock.writeLock().lock();  // We know that there's no readers or writers when we can get the writeLock
		// Note that we can't just re-use this lock.  It may have already been removed from the map.
		// We need a lock that we know is in the map and that we got the lock before another thread.
		lock.writeLock().unlock();
	}
}
