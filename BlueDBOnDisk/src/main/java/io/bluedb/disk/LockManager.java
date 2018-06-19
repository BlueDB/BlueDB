package io.bluedb.disk;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import io.bluedb.api.keys.BlueKey;

public class LockManager {
	Map<Object, Lock> locks = new HashMap<>();

	public void lock(Object o) {
		getLock(o).lock();
	}

	public void unlock(Object o) {
		getLock(o).unlock();
	}

	private Lock getLock(Object key) {
		synchronized(locks) {
			Lock lock = locks.get(key);
			if (lock != null) {
				return lock;
			}
			lock = new ReentrantLock();
			locks.put(key, lock);
			return lock;
		}
	}
}
