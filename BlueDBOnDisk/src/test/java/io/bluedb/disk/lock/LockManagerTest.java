package io.bluedb.disk.lock;

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import io.bluedb.disk.Blutils;

public class LockManagerTest {

	@Test
	public void test_read_while_read_locked() {
		LockManager<String> lockManager = new LockManager<>();
		String key = "key";
		AtomicBoolean secondReaderSuccess = new AtomicBoolean(false);
		Thread secondReader = new Thread(new Runnable() {
			@Override
			public void run() {
				try (BlueReadLock<String> lock = lockManager.acquireReadLock(key)) {
					secondReaderSuccess.set(true);
				}
			}
		});
		try (BlueReadLock<String> lock = lockManager.acquireReadLock(key)) {
			secondReader.start();
			Blutils.trySleep(20);
			assertTrue(secondReaderSuccess.get());  // no need to wait
		}
	}

	@Test
	public void test_read_while_write_locked() {
		LockManager<String> lockManager = new LockManager<>();
		String key = "key";
		AtomicBoolean readSuccess = new AtomicBoolean(false);
		Thread reader = new Thread(new Runnable() {
			@Override
			public void run() {
				try (BlueReadLock<String> lock = lockManager.acquireReadLock(key)) {
					readSuccess.set(true);
				}
			}
		});
		try (BlueWriteLock<String> lock = lockManager.acquireWriteLock(key)) {
			reader.start();
			assertFalse(readSuccess.get());
			Blutils.trySleep(20);
			assertFalse(readSuccess.get());
		}
		Blutils.trySleep(20);  // give the reader time to grab the lock;
		assertTrue(readSuccess.get());
	}

	@Test
	public void test_write_while_read_locked() {
		LockManager<String> lockManager = new LockManager<>();
		String key = "key";
		AtomicBoolean writeSuccess = new AtomicBoolean(false);
		Thread writer = new Thread(new Runnable() {
			@Override
			public void run() {
				try (BlueWriteLock<String> lock = lockManager.acquireWriteLock(key)) {
					writeSuccess.set(true);
				}
			}
		});
		try (BlueReadLock<String> lock = lockManager.acquireReadLock(key)) {
			writer.start();
			assertFalse(writeSuccess.get());
			Blutils.trySleep(20);
			assertFalse(writeSuccess.get());
		}
		Blutils.trySleep(20);  // give the writer time to grab the lock;
		assertTrue(writeSuccess.get());
	}

	@Test
	public void test_write_while_write_locked() {
		LockManager<String> lockManager = new LockManager<>();
		String key = "key";
		AtomicBoolean secondWriteSuccess = new AtomicBoolean(false);
		Thread secondWriter = new Thread(new Runnable() {
			@Override
			public void run() {
				try (BlueWriteLock<String> lock = lockManager.acquireWriteLock(key)) {
					secondWriteSuccess.set(true);
				}
			}
		});
		try (BlueWriteLock<String> lock = lockManager.acquireWriteLock(key)) {
			secondWriter.start();
			assertFalse(secondWriteSuccess.get());
			Blutils.trySleep(20);
			assertFalse(secondWriteSuccess.get());
		}
		Blutils.trySleep(20);  // give the second writer time to grab the lock;
		assertTrue(secondWriteSuccess.get());
	}
}
