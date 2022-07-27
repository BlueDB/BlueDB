package org.bluedb.api.keys;

import java.util.UUID;
import org.junit.Test;
import junit.framework.TestCase;

public class UUIDKeyTest extends TestCase {

	@Test
	public void test_getId() {
		UUIDKey key0 = new UUIDKey(new UUID(0, 0));
		UUIDKey key1 = new UUIDKey(new UUID(0, 1));
		UUIDKey nullKey = new UUIDKey(null);
		assertEquals(new UUID(0, 0), key0.getId());
		assertEquals(new UUID(0, 1), key1.getId());
		assertEquals(null, nullKey.getId());
	}

	@Test
	public void test_getGroupingNumber() {
		UUIDKey zero = new UUIDKey(new UUID(0, 0));
		UUIDKey one = new UUIDKey(new UUID(0, 1));
		UUIDKey oneCopy = new UUIDKey(new UUID(0, 1));
		UUIDKey nullKey = new UUIDKey(null);
		assertEquals(one.getGroupingNumber(), oneCopy.getGroupingNumber());
		assertTrue(one.getGroupingNumber() == oneCopy.getGroupingNumber());
		assertFalse(one.getGroupingNumber() == zero.getGroupingNumber());
		assertFalse(nullKey.getGroupingNumber() == one.getGroupingNumber());

		assertTrue(zero.getGroupingNumber() >= 0);
		assertTrue(one.getGroupingNumber() >= 0);

		long integerMaxValueAsLong = Integer.MAX_VALUE;
		assertTrue(zero.getGroupingNumber() <= integerMaxValueAsLong * 2 + 1);
		assertTrue(one.getGroupingNumber() <= integerMaxValueAsLong * 2 + 1);
	}

	@Test
	public void test_hashCode() {
		UUIDKey zero = new UUIDKey(new UUID(0, 0));
		UUIDKey one = new UUIDKey(new UUID(0, 1));
		UUIDKey oneCopy = new UUIDKey(new UUID(0, 1));
		UUIDKey nullKey = new UUIDKey(null);
		assertEquals(one.hashCode(), oneCopy.hashCode());
		assertTrue(one.hashCode() == oneCopy.hashCode());
		assertFalse(one.hashCode() == zero.hashCode());
		assertFalse(nullKey.hashCode() == one.hashCode());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void test_equals() {
		UUIDKey zero = new UUIDKey(new UUID(0, 0));
		UUIDKey one = new UUIDKey(new UUID(0, 1));
		UUIDKey oneCopy = new UUIDKey(new UUID(0, 1));
		UUIDKey nullKey = new UUIDKey(null);
		UUIDKey nullKey2 = new UUIDKey(null);
		assertEquals(one, one);
		assertEquals(one, oneCopy);
		assertTrue(one.equals(oneCopy));
		assertFalse(one.equals(zero));
		assertTrue(nullKey.equals(nullKey2));
		assertFalse(one.equals(null));
		assertFalse(one.equals(nullKey));
		assertFalse(nullKey.equals(one));
		assertFalse(one.equals(new UUID(0, 1)));
	}

	@Test
	public void test_toString() {
		UUIDKey zero = new UUIDKey(new UUID(0, 0));
		UUIDKey one = new UUIDKey(new UUID(0, 1));
		assertTrue(zero.toString().contains(String.valueOf(zero)));
		assertTrue(one.toString().contains(String.valueOf(one.getId())));

		assertTrue(one.toString().contains(one.getClass().getSimpleName()));
}

	@Test
	public void test_compareTo() {
		UUIDKey zero = new UUIDKey(new UUID(0, 0));
		UUIDKey one = new UUIDKey(new UUID(0, 1));
		UUIDKey oneCopy = new UUIDKey(new UUID(0, 1));
		LongKey longKey = new LongKey(1);
		assertTrue(zero.compareTo(one) != 0);  // basic
		assertTrue(one.compareTo(zero) != 0);  // reverse
		assertTrue(one.compareTo(oneCopy) == 0);  // equals
		assertTrue(one.compareTo(null) != 0);  // sanity check
		assertTrue(one.compareTo(longKey) != 0);  // sanity check
	}

	@Test
	public void test_getLongIdIfPresent() {
		UUIDKey uuidKey = new UUIDKey(UUID.randomUUID());
		assertNull(uuidKey.getLongIdIfPresent());
	}

	@Test
	public void test_getIntegerIdIfPresent() {
		UUIDKey uuidKey = new UUIDKey(UUID.randomUUID());
		assertNull(uuidKey.getIntegerIdIfPresent());
	}

	@Test
	public void test_getStringIdIfPresent() {
		UUIDKey uuidKey = new UUIDKey(UUID.randomUUID());
		assertNull(uuidKey.getStringIdIfPresent());
	}

	@Test
	public void test_getUUIDIdIfPresent() {
		UUIDKey uuidKey = new UUIDKey(UUID.randomUUID());
		assertEquals(uuidKey.getId(), uuidKey.getUUIDIdIfPresent());
	}

	@Test
	public void test_isInRange() {
		UUIDKey UUIDKey = new UUIDKey(new UUID(0, 1));
		long groupingNumber = UUIDKey.getGroupingNumber();
		assertFalse(UUIDKey.overlapsRange(groupingNumber - 1, groupingNumber - 1));
		assertTrue(UUIDKey.overlapsRange(groupingNumber - 1, groupingNumber));
		assertTrue(UUIDKey.overlapsRange(groupingNumber - 1, groupingNumber + 1));
		assertTrue(UUIDKey.overlapsRange(groupingNumber, groupingNumber));
		assertTrue(UUIDKey.overlapsRange(groupingNumber, groupingNumber + 1));
		assertFalse(UUIDKey.overlapsRange(groupingNumber + 1, groupingNumber + 1));
	}

	@Test
	public void test_isAfterRange() {
		UUIDKey UUIDKey = new UUIDKey(new UUID(0, 1));
		long groupingNumber = UUIDKey.getGroupingNumber();
		assertTrue(UUIDKey.isAfterRange(groupingNumber - 1, groupingNumber - 1));
		assertFalse(UUIDKey.isAfterRange(groupingNumber - 1, groupingNumber));
		assertFalse(UUIDKey.isAfterRange(groupingNumber - 1, groupingNumber + 1));
		assertFalse(UUIDKey.isAfterRange(groupingNumber, groupingNumber));
		assertFalse(UUIDKey.isAfterRange(groupingNumber, groupingNumber + 1));
		assertFalse(UUIDKey.isAfterRange(groupingNumber + 1, groupingNumber + 1));
	}
	
	@Test
	public void test_isActiveTimeKey() {
		UUIDKey UUIDKey = new UUIDKey(new UUID(0, 1));
		assertFalse(UUIDKey.isActiveTimeKey());
	}
}
