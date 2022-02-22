package org.bluedb.api;

import static org.junit.Assert.*;

import org.bluedb.api.exceptions.BlueDbException;
import org.junit.Test;

public class CloseableIteratorTest {

	@Test
	public void test_countRemainderAndClose_errorCounting() {
		@SuppressWarnings("resource")
		CloseableIterator<?> iter = new CloseableIterator<Object>() {
			@Override
			public boolean hasNext() {
				throw new RuntimeException();
			}
			@Override public void close() {}
			@Override public Object next() { return null; }
			@Override public Object peek() { return null; }
			@Override public void keepAlive() {}
		};
		try {
			iter.countRemainderAndClose();
			fail();
		} catch (BlueDbException e) {}  // should throw BlueDbException rather than RuntimeException
	}
}
