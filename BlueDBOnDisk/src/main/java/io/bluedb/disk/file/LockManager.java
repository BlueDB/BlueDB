package io.bluedb.disk.file;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class LockManager <T> {
	private final Map<T, CountDownLatch> latches;

	public LockManager() {
		latches = new HashMap<>();
	}

	public void acquire(T key) {
		CountDownLatch myLatch = new CountDownLatch(1);
		CountDownLatch latchInMap;
		do {
			latchInMap = tryInsertMyLatch(key, myLatch);
			if (latchInMap != myLatch) {
				waitForLatch(latchInMap);
			}
		} while (latchInMap != myLatch);
	}

	public void release(T key) {
		CountDownLatch latch;
		synchronized(latches) {
			latch = latches.remove(key);
		}
		latch.countDown();
	}

	private CountDownLatch tryInsertMyLatch(T key, CountDownLatch myLatch) {
		synchronized(latches) {
			CountDownLatch latchAlreadyInMap = latches.get(key);
			if (latchAlreadyInMap != null) {
				return latchAlreadyInMap;
			} else {
				latches.put(key, myLatch);
				return myLatch;
			}
		}
	}

	private void waitForLatch(CountDownLatch latch) {
		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
