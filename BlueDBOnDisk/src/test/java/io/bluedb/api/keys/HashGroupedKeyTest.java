package io.bluedb.api.keys;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class HashGroupedKeyTest {

	TestHashable value1Hash3 = new TestHashable(1, 3);
	TestHashable value2Hash2 = new TestHashable(2, 2);
	TestHashable value3Hash2 = new TestHashable(3, 2);

	TestHashedKey key1Hash3 = new TestHashedKey(value1Hash3);
	TestHashedKey key2Hash2 = new TestHashedKey(value2Hash2);
	TestHashedKey key3Hash2 = new TestHashedKey(value3Hash2);

	@Test
	public void test_sort() throws Exception {
		List<TestHashedKey> sorted = Arrays.asList(key2Hash2, key3Hash2, key1Hash3);
		List<TestHashedKey> listToSort = Arrays.asList(key1Hash3, key3Hash2, key2Hash2);
		Collections.sort(listToSort);
		assertEquals(sorted, listToSort);
	}

	@Test
	public void test_compareTo() throws Exception {
		assertTrue(key1Hash3.compareTo(key1Hash3) == 0);
		assertTrue(key1Hash3.compareTo(key2Hash2) > 0);  // larger hash
		assertTrue(key1Hash3.compareTo(key3Hash2) > 0);  // larger hash

		assertTrue(key2Hash2.compareTo(key1Hash3) < 0); // smaller hash
		assertTrue(key2Hash2.compareTo(key2Hash2) == 0);
		assertTrue(key2Hash2.compareTo(key3Hash2) < 0);

		assertTrue(key3Hash2.compareTo(key1Hash3) < 0); // smaller hash
		assertTrue(key3Hash2.compareTo(key2Hash2) > 0);
		assertTrue(key3Hash2.compareTo(key3Hash2) == 0);
	}

	@Test
	public void test_compareTo_differentSubClasses() throws Exception {
		TestHashedKeySubClass subKey1Hash3 = new TestHashedKeySubClass(value1Hash3);
		TestHashedKeySubClass subKey2Hash2 = new TestHashedKeySubClass(value2Hash2);
		assertTrue(key2Hash2.compareTo(subKey2Hash2) != 0);
		assertTrue(key2Hash2.compareTo(subKey1Hash3) < 0);

	}

	@SuppressWarnings("serial")
	private class TestHashedKeySubClass extends TestHashedKey {
		TestHashedKeySubClass(TestHashable value) {
			super(value);
		}
	}

	@SuppressWarnings("serial")
	private class TestHashedKey extends HashGroupedKey<TestHashable> {
		final TestHashable value;
		TestHashedKey(TestHashable value) {
			this.value = value;
		}
		@Override
		public TestHashable getId() {
			return value;
		}
		
	}

	private class TestHashable implements Comparable<TestHashable> {
		private final int compareValue;
		private final int hashValue;

		TestHashable(int compareValue, int hashValue) {
			this.compareValue = compareValue;
			this.hashValue = hashValue;
		}
		@Override
		public int compareTo(TestHashable o) {
			return Integer.compare(compareValue, o.compareValue);
		}
		@Override
		public int hashCode() {
			return hashValue;
		}
	}
}
