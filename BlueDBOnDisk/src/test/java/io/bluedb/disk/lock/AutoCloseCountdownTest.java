package io.bluedb.disk.lock;

import static org.junit.Assert.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import io.bluedb.disk.Blutils;
import io.bluedb.disk.lock.AutoCloseCountdown;

public class AutoCloseCountdownTest {

	@Test
	public void test_AutoCloseCountdown() {
		TestingCloseable closeable;

		try {
			new AutoCloseCountdown(null, 100);  // can't open with a null
			fail();
		} catch(IllegalArgumentException e) {}

		closeable = new TestingCloseable(); 
		new AutoCloseCountdown(closeable, 0);
		assertTrue(closeable.isClosed());

		closeable = new TestingCloseable(); 
		new AutoCloseCountdown(closeable, 1);
		assertTrue(successfullyCloses(closeable, 2000));

		closeable = new TestingCloseable(); 
		new AutoCloseCountdown(closeable, 3000);
		assertFalse(successfullyCloses(closeable, 100));
	}

	@Test
	public void test_snooze() {
		TestingCloseable closeable = new TestingCloseable();
		AutoCloseCountdown countDown = new AutoCloseCountdown(closeable, 200);
		assertTrue(countDown.remainingTime() > 150);
		Blutils.trySleep(50);
		assertFalse(countDown.remainingTime() > 150);
		countDown.snooze();
		assertTrue(countDown.remainingTime() > 150);
	}

	@Test
	public void test_cancel() {
		TestingCloseable closeable = new TestingCloseable(); 
		AutoCloseCountdown countDown = new AutoCloseCountdown(closeable, 10);
		countDown.cancel();
		assertFalse(successfullyCloses(closeable, 20));

	}

	@Test
	public void test_remainingTime() {
		TestingCloseable closeable = new TestingCloseable();
		AutoCloseCountdown countDown = new AutoCloseCountdown(closeable, 200);
		assertTrue(countDown.remainingTime() > 150);
		Blutils.trySleep(50);
		assertFalse(countDown.remainingTime() > 150);
		countDown.snooze();
		assertTrue(countDown.remainingTime() > 150);
	}

	@Test
	public void test_closeTarget_exception() {
		Closeable exceptionCloseable = new Closeable() {
			@Override
			public void close() throws IOException {
			}
		};

		try {
			AutoCloseCountdown countDown = new AutoCloseCountdown(exceptionCloseable, 0);
		} catch (Exception e) {
			fail();
		}
	}

	private boolean successfullyCloses(TestingCloseable closeable, long timeLimit) {
		long legSize = 10;
		for (int i=0; i < timeLimit / legSize; i++) {
			if (closeable.isClosed()) {
				return true;
			}
			Blutils.trySleep(legSize);
		}
		return false;
	}

	private class TestingCloseable implements Closeable {
		AtomicBoolean countdownCompleted = new AtomicBoolean(false);

		@Override
		public void close() {
			countdownCompleted.set(true);
		}
		public boolean isClosed() {
			return countdownCompleted.get();
		}
	};

}
