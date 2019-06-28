package org.bluedb.tasks;

@FunctionalInterface
public interface ThrowingRunnable {
	void run() throws Throwable;
}
