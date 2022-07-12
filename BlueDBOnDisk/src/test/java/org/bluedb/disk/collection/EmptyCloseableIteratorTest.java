package org.bluedb.disk.collection;

import static org.junit.Assert.*;

import org.junit.Test;

public class EmptyCloseableIteratorTest {

	@Test
	public void test_returnsEmptyResultsAndDoesNotThrowExceptions() {
		EmptyCloseableIterator<Integer> emptyCloseableIterator = new EmptyCloseableIterator<Integer>();
		assertFalse(emptyCloseableIterator.hasNext());
		assertNull(emptyCloseableIterator.peek());
		assertNull(emptyCloseableIterator.next());
		emptyCloseableIterator.keepAlive();
		emptyCloseableIterator.close();
	}

}
