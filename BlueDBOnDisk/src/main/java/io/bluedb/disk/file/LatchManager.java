package io.bluedb.disk.file;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class LatchManager <T> {
	private final Map<T, CountDownLatch> latches;

	public LatchManager() {
		latches = new HashMap<>();
	}
	
	public void requestLatchFor(T key) {
		CountDownLatch latch;
		boolean hasLock = false;
		while (!hasLock) {
			synchronized(latches) {
				latch = latches.get(key);
				if (latch == null) {
					latches.put(key, new CountDownLatch(1));
					hasLock = true;
				}
			}
			if (latch != null) {
				try {
					latch.await();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
			}
		}
	}
	
	public void releaseLatch(T key) {
		CountDownLatch latch;
		synchronized(latches) {
			latch = latches.remove(key);
		}
		latch.countDown();
	}
}
