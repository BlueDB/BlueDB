package org.bluedb.disk.serialization;

import static org.junit.Assert.*;

import java.util.UUID;

import org.junit.Test;

import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.UUIDKey;
import org.bluedb.disk.TestValue;

public class BlueEntityTest {

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void test_equals() {
		LongKey zeroLongKey = new LongKey(0);
		LongKey oneLongKey = new LongKey(1);
		LongKey oneLongKeyCopy = new LongKey(1);
		StringKey stringKey = new StringKey("1");
		UUIDKey uuidKey = new UUIDKey(UUID.randomUUID());
		TestValue joe = new TestValue("joe");
		TestValue bob = new TestValue("bob");
		TestValue bobCopy = new TestValue("bob");
		
		BlueEntity<TestValue> joeWithOneLongKey = new BlueEntity<>(oneLongKey, joe);
		BlueEntity<TestValue> bobWithOneLongKey = new BlueEntity<>(oneLongKey, bob);
		BlueEntity<TestValue> bobWithOneLongKeyCopy = new BlueEntity<>(oneLongKeyCopy, bobCopy);
		BlueEntity<TestValue> bobWithZeroLongKey = new BlueEntity<>(zeroLongKey, bob);
		BlueEntity<TestValue> bobWitStringKey = new BlueEntity<>(stringKey, bob);
		BlueEntity<TestValue> bobWitUuidKey = new BlueEntity<>(uuidKey, bob);
		BlueEntity<TestValue> bobWithNullKey = new BlueEntity<>(null, bob);
		BlueEntity<TestValue> nullWithOneLongKey = new BlueEntity<>(oneLongKey, null);
		
		assertTrue(bobWithOneLongKeyCopy.equals(bobWithOneLongKey));
		assertTrue(bobWithOneLongKey.equals(bobWithOneLongKeyCopy));
		
		assertFalse(bobWithOneLongKey.equals(joeWithOneLongKey));
		assertFalse(bobWithOneLongKey.equals(bobWithZeroLongKey));
		assertFalse(bobWithOneLongKey.equals(bobWitStringKey));
		assertFalse(bobWithOneLongKey.equals(bobWitUuidKey));
		assertFalse(bobWithOneLongKey.equals(bobWithNullKey));
		assertFalse(bobWithOneLongKey.equals(nullWithOneLongKey));
		assertFalse(bobWithOneLongKey.equals(null));
		assertFalse(bobWithOneLongKey.equals(bob));

		assertFalse(joeWithOneLongKey.equals(bobWithOneLongKey));
		assertFalse(bobWithZeroLongKey.equals(bobWithOneLongKey));
		assertFalse(bobWitStringKey.equals(bobWithOneLongKey));
		assertFalse(bobWitUuidKey.equals(bobWithOneLongKey));
		assertFalse(bobWithNullKey.equals(bobWithOneLongKey));
		assertFalse(nullWithOneLongKey.equals(bobWithOneLongKey));
	}

	@Test
	public void test_hashCode() {
		LongKey zeroLongKey = new LongKey(0);
		LongKey oneLongKey = new LongKey(1);
		LongKey oneLongKeyCopy = new LongKey(1);
		StringKey stringKey = new StringKey("1");
		UUIDKey uuidKey = new UUIDKey(UUID.randomUUID());
		TestValue joe = new TestValue("joe");
		TestValue bob = new TestValue("bob");
		TestValue bobCopy = new TestValue("bob");
		
		BlueEntity<TestValue> joeWithOneLongKey = new BlueEntity<>(oneLongKey, joe);
		BlueEntity<TestValue> bobWithOneLongKey = new BlueEntity<>(oneLongKey, bob);
		BlueEntity<TestValue> bobWithOneLongKeyCopy = new BlueEntity<>(oneLongKeyCopy, bobCopy);
		BlueEntity<TestValue> bobWithZeroLongKey = new BlueEntity<>(zeroLongKey, bob);
		BlueEntity<TestValue> bobWitStringKey = new BlueEntity<>(stringKey, bob);
		BlueEntity<TestValue> bobWitUuidKey = new BlueEntity<>(uuidKey, bob);
		BlueEntity<TestValue> bobWithNullKey = new BlueEntity<>(null, bob);
		BlueEntity<TestValue> nullWithOneLongKey = new BlueEntity<>(oneLongKey, null);
		
		assertEquals(bobWithOneLongKeyCopy.hashCode(), bobWithOneLongKey.hashCode());

		assertFalse(bobWithOneLongKey.hashCode() == joeWithOneLongKey.hashCode());
		assertFalse(bobWithOneLongKey.hashCode() == bobWithZeroLongKey.hashCode());
		assertFalse(bobWithOneLongKey.hashCode() == bobWitStringKey.hashCode());
		assertFalse(bobWithOneLongKey.hashCode() == bobWitUuidKey.hashCode());
		assertFalse(bobWithOneLongKey.hashCode() == bobWithNullKey.hashCode());
		assertFalse(bobWithOneLongKey.hashCode() == nullWithOneLongKey.hashCode());
		assertFalse(bobWithOneLongKey.hashCode() == bob.hashCode());

		assertFalse(joeWithOneLongKey.hashCode() == bobWithOneLongKey.hashCode());
		assertFalse(bobWithZeroLongKey.hashCode() == bobWithOneLongKey.hashCode());
		assertFalse(bobWitStringKey.hashCode() == bobWithOneLongKey.hashCode());
		assertFalse(bobWitUuidKey.hashCode() == bobWithOneLongKey.hashCode());
		assertFalse(bobWithNullKey.hashCode() == bobWithOneLongKey.hashCode());
		assertFalse(nullWithOneLongKey.hashCode() == bobWithOneLongKey.hashCode());
	}

	@Test
	public void test_compareTo() {
		LongKey zeroLongKey = new LongKey(0);
		LongKey oneLongKey = new LongKey(1);
		LongKey oneLongKeyCopy = new LongKey(1);
		StringKey stringKey = new StringKey("1");
		UUIDKey uuidKey = new UUIDKey(UUID.randomUUID());
		TestValue joe = new TestValue("joe");
		TestValue bob = new TestValue("bob");
		TestValue bobCopy = new TestValue("bob");
		
		BlueEntity<TestValue> joeWithOneLongKey = new BlueEntity<>(oneLongKey, joe);
		BlueEntity<TestValue> bobWithOneLongKey = new BlueEntity<>(oneLongKey, bob);
		BlueEntity<TestValue> bobWithOneLongKeyCopy = new BlueEntity<>(oneLongKeyCopy, bobCopy);
		BlueEntity<TestValue> bobWithZeroLongKey = new BlueEntity<>(zeroLongKey, bob);
		BlueEntity<TestValue> bobWitStringKey = new BlueEntity<>(stringKey, bob);
		BlueEntity<TestValue> bobWitUuidKey = new BlueEntity<>(uuidKey, bob);
		BlueEntity<TestValue> bobWithNullKey = new BlueEntity<>(null, bob);
		BlueEntity<TestValue> bobWithNullKeyCopy = new BlueEntity<>(null, bob);
		BlueEntity<TestValue> nullWithOneLongKey = new BlueEntity<>(oneLongKey, null);

		assertEquals(0, bobWithOneLongKey.compareTo(bobWithOneLongKeyCopy));

		assertEquals(0, bobWithOneLongKey.compareTo(joeWithOneLongKey));
		assertEquals(0, joeWithOneLongKey.compareTo(bobWithOneLongKey));
		assertEquals(0, bobWithNullKey.compareTo(bobWithNullKeyCopy));

		assertTrue(bobWithOneLongKey.compareTo(bobWithZeroLongKey) > 0);
		assertTrue(bobWithZeroLongKey.compareTo(bobWithOneLongKey) < 0);

		assertTrue(bobWithZeroLongKey.compareTo(bobWitStringKey) != 0);
		assertTrue(bobWithZeroLongKey.compareTo(bobWitUuidKey) != 0);
		assertTrue(bobWithZeroLongKey.compareTo(bobWithNullKey) != 0);
		assertTrue(bobWithZeroLongKey.compareTo(nullWithOneLongKey) != 0);

		assertTrue(bobWitStringKey.compareTo(bobWithZeroLongKey) != 0);
		assertTrue(bobWitUuidKey.compareTo(bobWithZeroLongKey) != 0);
		assertTrue(bobWithNullKey.compareTo(bobWithZeroLongKey) != 0);
		assertTrue(nullWithOneLongKey.compareTo(bobWithZeroLongKey) != 0);
	}

	@Test
	public void test_toString() {
		LongKey oneLongKey = new LongKey(1);
		TestValue bob = new TestValue("bob");
		
		BlueEntity<TestValue> bobWithOneLongKey = new BlueEntity<>(oneLongKey, bob);
		BlueEntity<TestValue> bobWithNullKey = new BlueEntity<>(null, bob);
		BlueEntity<TestValue> nullWithOneLongKey = new BlueEntity<>(oneLongKey, null);
		
		assertTrue(bobWithOneLongKey.toString().contains(bob.toString()));
		assertTrue(bobWithOneLongKey.toString().contains(oneLongKey.toString()));
		assertTrue(bobWithOneLongKey.toString().contains(bobWithOneLongKey.getClass().getSimpleName()));

		assertTrue(bobWithNullKey.toString().contains(bob.toString()));
		assertTrue(bobWithNullKey.toString().contains("null"));
		assertTrue(bobWithNullKey.toString().contains(bobWithNullKey.getClass().getSimpleName()));

		assertTrue(nullWithOneLongKey.toString().contains("null"));
		assertTrue(nullWithOneLongKey.toString().contains(oneLongKey.toString()));
		assertTrue(nullWithOneLongKey.toString().contains(nullWithOneLongKey.getClass().getSimpleName()));
	}
}
