package org.bluedb.api.keys;

import java.util.UUID;
import org.junit.Test;
import junit.framework.TestCase;

public class LongTimeKeyTest extends TestCase {

	@Test
	public void test_getId() {
		LongTimeKey min = new LongTimeKey(Long.MIN_VALUE);
		LongTimeKey max = new LongTimeKey(Long.MAX_VALUE);
		LongTimeKey zero = new LongTimeKey(0);
		LongTimeKey one = new LongTimeKey(1);
		assertEquals(Long.MIN_VALUE, min.getId());
		assertEquals(Long.MAX_VALUE, max.getId());
		assertEquals(0, zero.getId());
		assertEquals(1, one.getId());
	}

	@Test
	public void test_getGroupingNumber() {
		TimeKey min = new TimeKey(1, Long.MIN_VALUE);
		TimeKey max = new TimeKey(2, Long.MAX_VALUE);
		TimeKey zero = new TimeKey(3, 0);
		TimeKey one = new TimeKey(4, 1);
		TimeKey oneCopy = new TimeKey(4, 1);
		TimeKey oneDifferent = new TimeKey(4, 1);
		assertEquals(one.getGroupingNumber(), oneCopy.getGroupingNumber());
		assertTrue(one.getGroupingNumber() == oneCopy.getGroupingNumber());
		assertTrue(one.getGroupingNumber() == oneDifferent.getGroupingNumber());
		assertFalse(one.getGroupingNumber() == zero.getGroupingNumber());
		assertFalse(min.getGroupingNumber() == max.getGroupingNumber());
		assertFalse(max.getGroupingNumber() == zero.getGroupingNumber());
		assertFalse(min.getGroupingNumber() == zero.getGroupingNumber());
	}

	@Test
	public void test_hashCode() {
		LongTimeKey min = new LongTimeKey(Long.MIN_VALUE);
		LongTimeKey max = new LongTimeKey(Long.MAX_VALUE);
		LongTimeKey zero = new LongTimeKey(0);
		LongTimeKey one = new LongTimeKey(1);
		LongTimeKey oneCopy = new LongTimeKey(1);
		assertEquals(one.hashCode(), oneCopy.hashCode());
		assertTrue(one.hashCode() == oneCopy.hashCode());
		assertFalse(one.hashCode() == zero.hashCode());
		assertFalse(min.hashCode() == zero.hashCode());
//		assertFalse(min.hashCode() == max.hashCode());  // Actually, true.  In fact, Long.hashCode(Long.MAX_VALUE) == Long.hashCode(Long.MIN_VALUE)
		assertFalse(max.hashCode() == zero.hashCode());
		assertFalse(min.hashCode() == zero.hashCode());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void test_equals() {
		LongTimeKey min = new LongTimeKey(Long.MIN_VALUE);
		LongTimeKey max = new LongTimeKey(Long.MAX_VALUE);
		LongTimeKey zero = new LongTimeKey(0);
		LongTimeKey one = new LongTimeKey(1);
		LongTimeKey oneCopy = new LongTimeKey(1);
		StringKey stringKey = new StringKey("1");
		UUIDKey uuidKey = new UUIDKey(UUID.randomUUID());
		assertEquals(one, one);
		assertEquals(one, oneCopy);
		assertTrue(one.equals(oneCopy));
		assertFalse(one.equals(zero));
		assertFalse(min.equals(max));
		assertFalse(one.equals(stringKey));
		assertFalse(one.equals(uuidKey));
		assertFalse(one.equals(null));
		assertFalse(one.equals("1"));
	}

	@Test
	public void test_toString() {
		LongTimeKey min = new LongTimeKey(Long.MIN_VALUE);
		LongTimeKey max = new LongTimeKey(Long.MAX_VALUE);
		LongTimeKey zero = new LongTimeKey(0);
		LongTimeKey one = new LongTimeKey(1);
		assertTrue(min.toString().contains(String.valueOf(min.getId())));
		assertTrue(max.toString().contains(String.valueOf(max.getId())));
		assertTrue(zero.toString().contains(String.valueOf(zero)));
		assertTrue(one.toString().contains(String.valueOf(one.getId())));

		assertTrue(min.toString().contains(min.getClass().getSimpleName()));
}

	@Test
	public void test_compareTo() {
		LongTimeKey min = new LongTimeKey(Long.MIN_VALUE);
		LongTimeKey max = new LongTimeKey(Long.MAX_VALUE);
		LongTimeKey zero = new LongTimeKey(0);
		LongTimeKey one = new LongTimeKey(1);
		LongTimeKey oneCopy = new LongTimeKey(1);
		LongTimeKey two = new LongTimeKey(4);
		StringKey stringKey = new StringKey("1");
		UUIDKey uuidKey = new UUIDKey(UUID.randomUUID());
		TimeKey timeKeyWithGroupingNumberMatchingTwo = new TimeKey(5, 4611686018427387906l);
		assertTrue(zero.compareTo(one) < 0);  // basic
		assertTrue(one.compareTo(zero) > 0);  // reverse
		assertTrue(min.compareTo(max) < 0);  // extreme
		assertTrue(max.compareTo(min) > 0);  // extreme backwards
		assertTrue(one.compareTo(oneCopy) == 0);  // equals
		assertTrue(one.compareTo(null) != 0);  // sanity check
		assertTrue(one.compareTo(stringKey) != 0);  // sanity check
		assertTrue(one.compareTo(uuidKey) != 0);  // sanity check
		assertTrue(two.compareTo(timeKeyWithGroupingNumberMatchingTwo) < 0);  // same grouping number, different class
		
		assertTrue(one.postGroupingNumberCompareTo(uuidKey) < 0);
	}


	@Test
	public void test_getLongIdIfPresent() {
		LongTimeKey longTimeKey = new LongTimeKey(1);
		assertEquals(Long.valueOf(1), longTimeKey.getLongIdIfPresent());	}

	@Test
	public void test_getIntegerIdIfPresent() {
		LongTimeKey longTimeKey = new LongTimeKey(1);
		assertNull(longTimeKey.getIntegerIdIfPresent());
	}

	@Test
	public void test_getStringIdIfPresent() {
		LongTimeKey longTimeKey = new LongTimeKey(1);
		assertNull(longTimeKey.getStringIdIfPresent());
	}

	@Test
	public void test_getUUIDIdIfPresent() {
		LongTimeKey longTimeKey = new LongTimeKey(1);
		assertNull(longTimeKey.getUUIDIdIfPresent());
	}

	@Test
	public void test_isInRange() {
		LongTimeKey longTimeKey = new LongTimeKey(1);
		long groupingNumber = longTimeKey.getGroupingNumber();
		assertFalse(longTimeKey.overlapsRange(groupingNumber - 1, groupingNumber - 1));
		assertTrue(longTimeKey.overlapsRange(groupingNumber - 1, groupingNumber));
		assertTrue(longTimeKey.overlapsRange(groupingNumber - 1, groupingNumber + 1));
		assertTrue(longTimeKey.overlapsRange(groupingNumber, groupingNumber));
		assertTrue(longTimeKey.overlapsRange(groupingNumber, groupingNumber + 1));
		assertFalse(longTimeKey.overlapsRange(groupingNumber + 1, groupingNumber + 1));
	}

	@Test
	public void test_isAfterRange() {
		LongTimeKey longTimeKey = new LongTimeKey(1);
		long groupingNumber = longTimeKey.getGroupingNumber();
		assertTrue(longTimeKey.isAfterRange(groupingNumber - 1, groupingNumber - 1));
		assertFalse(longTimeKey.isAfterRange(groupingNumber - 1, groupingNumber));
		assertFalse(longTimeKey.isAfterRange(groupingNumber - 1, groupingNumber + 1));
		assertFalse(longTimeKey.isAfterRange(groupingNumber, groupingNumber));
		assertFalse(longTimeKey.isAfterRange(groupingNumber, groupingNumber + 1));
		assertFalse(longTimeKey.isAfterRange(groupingNumber + 1, groupingNumber + 1));
	}
	
	@Test
	public void test_isActiveTimeKey() {
		LongTimeKey longTimeKey = new LongTimeKey(1);
		assertFalse(longTimeKey.isActiveTimeKey());
	}
}
