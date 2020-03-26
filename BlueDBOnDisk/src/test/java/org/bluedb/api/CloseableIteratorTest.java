package org.bluedb.api;

import static org.junit.Assert.*;

import java.io.IOException;

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
			@Override public void close() throws IOException {}
			@Override public Object next() { return null; }
			@Override public Object peek() { return null; }
			@Override public void keepAlive() {}
		};
		try {
			iter.countRemainderAndClose();
			fail();
		} catch (BlueDbException e) {}  // should throw BlueDbException rather than IOException
	}


	@Test
	public void test_countRemainderAndClose_errorClosing() {
		@SuppressWarnings("resource")
		CloseableIterator<?> iter = new CloseableIterator<Object>() {
			@Override public void close() throws IOException {
				throw new IOException();
			}
			@Override public boolean hasNext() { return false; }
			@Override public Object next() { return null; }
			@Override public Object peek() { return null; }
			@Override public void keepAlive() {}
		};
		try {
			iter.countRemainderAndClose();
			fail();
		} catch (BlueDbException e) {}  // should throw BlueDbException rather than IOException
	}
}
