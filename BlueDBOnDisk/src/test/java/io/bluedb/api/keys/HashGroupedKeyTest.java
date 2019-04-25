package io.bluedb.api.keys;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

		TestKeyType<String> genericTypeKeyInstance1 = new TestKeyType<>("1");
		TestKeyType<Integer> genericTypeKeyInstance2 = new TestKeyType<>(1);
		TestKeyType<Integer> genericTypeKeyInstanceNull = new TestKeyType<>(null);
		assertTrue( genericTypeKeyInstance1.compareTo(genericTypeKeyInstance2) != 0);
		assertTrue( genericTypeKeyInstance1.compareTo(genericTypeKeyInstanceNull) != 0);
		assertTrue( genericTypeKeyInstanceNull.compareTo(genericTypeKeyInstance1) != 0);
		assertTrue( genericTypeKeyInstance1.compareTo(null) != 0);
		assertEquals(genericTypeKeyInstance1.compareTo(genericTypeKeyInstance2), -genericTypeKeyInstance2.compareTo(genericTypeKeyInstance1));
		assertTrue( genericTypeKeyInstance1.compareTo(null) != 0);
	}

	@Test
	public void test_compareTo_differentSubClasses() throws Exception {
		TestHashedKeySubClass subKey1Hash3 = new TestHashedKeySubClass(value1Hash3);
		TestHashedKeySubClass subKey2Hash2 = new TestHashedKeySubClass(value2Hash2);
		assertTrue(key2Hash2.compareTo(subKey2Hash2) != 0);
		assertTrue(key2Hash2.compareTo(subKey1Hash3) < 0);
	}
	
	@Test
	public void test_compareTo_nullIds() {
		TestKeyType<Integer> nullKey1 = new TestKeyType<>(null);
		TestKeyType<Integer> nullKey2 = new TestKeyType<>(null);
		TestKeyType<Integer> validKey1 = new TestKeyType<>(0); //Number has to be 0 in order to have the same grouping number as null so that the compare has to check the ids
		
		assertEquals(-1, validKey1.compareTo(nullKey1));
		assertEquals(1, nullKey1.compareTo(validKey1));
		assertEquals(0, nullKey1.compareTo(nullKey2));
	}
	
	@Test
	public void test_compareTo_differentIdTypes() {
		TestKeyType<Integer> key1 = new TestKeyType<>(10);
		TestKeyType<Long> key2 = new TestKeyType<>(10l);
		
		assertTrue(key1.compareTo(key2) < 0);
		assertTrue(key2.compareTo(key1) > 0);
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

	private class TestKeyType<T extends Comparable<T>> extends HashGroupedKey<T> {
		private static final long serialVersionUID = 1L;
		private T id;
		TestKeyType(T id) {
			this.id = id;
		}
		@Override
		public T getId() {
			return id;
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
