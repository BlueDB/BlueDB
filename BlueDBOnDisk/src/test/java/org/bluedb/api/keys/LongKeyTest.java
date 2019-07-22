package org.bluedb.api.keys;

import java.util.UUID;
import org.junit.Test;
import junit.framework.TestCase;

public class LongKeyTest extends TestCase {

	@Test
	public void test_getId() {
		LongKey min = new LongKey(Long.MIN_VALUE);
		LongKey max = new LongKey(Long.MAX_VALUE);
		LongKey zero = new LongKey(0);
		LongKey one = new LongKey(1);
		assertEquals(Long.MIN_VALUE, min.getId());
		assertEquals(Long.MAX_VALUE, max.getId());
		assertEquals(0, zero.getId());
		assertEquals(1, one.getId());
	}

	@Test
	public void test_getGroupingNumber() {
		LongKey min = new LongKey(Long.MIN_VALUE);
		LongKey max = new LongKey(Long.MAX_VALUE);
		LongKey zero = new LongKey(0);
		LongKey one = new LongKey(1);
		LongKey oneCopy = new LongKey(1);
		assertEquals(one.getGroupingNumber(), oneCopy.getGroupingNumber());
		assertTrue(one.getGroupingNumber() == oneCopy.getGroupingNumber());
		assertFalse(max.getGroupingNumber() == zero.getGroupingNumber());
		assertFalse(min.getGroupingNumber() == zero.getGroupingNumber());

		assertTrue(min.getGroupingNumber() >= 0);
		assertTrue(max.getGroupingNumber() >= 0);
		assertTrue(zero.getGroupingNumber() >= 0);
		assertTrue(one.getGroupingNumber() >= 0);

		assertTrue(min.getGroupingNumber() < zero.getGroupingNumber());
		assertTrue(zero.getGroupingNumber() <= one.getGroupingNumber());
		assertTrue(one.getGroupingNumber() <= max.getGroupingNumber());
	}

	@Test
	public void test_hashCode() {
		LongKey min = new LongKey(Long.MIN_VALUE);
		LongKey max = new LongKey(Long.MAX_VALUE);
		LongKey zero = new LongKey(0);
		LongKey one = new LongKey(1);
		LongKey oneCopy = new LongKey(1);
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
		LongKey min = new LongKey(Long.MIN_VALUE);
		LongKey max = new LongKey(Long.MAX_VALUE);
		LongKey zero = new LongKey(0);
		LongKey one = new LongKey(1);
		LongKey oneCopy = new LongKey(1);
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
		LongKey min = new LongKey(Long.MIN_VALUE);
		LongKey max = new LongKey(Long.MAX_VALUE);
		LongKey zero = new LongKey(0);
		LongKey one = new LongKey(1);
		assertTrue(min.toString().contains(String.valueOf(min.getId())));
		assertTrue(max.toString().contains(String.valueOf(max.getId())));
		assertTrue(zero.toString().contains(String.valueOf(zero)));
		assertTrue(one.toString().contains(String.valueOf(one.getId())));

		assertTrue(min.toString().contains(min.getClass().getSimpleName()));
}

	@Test
	public void test_compareTo() {
		LongKey min = new LongKey(Long.MIN_VALUE);
		LongKey max = new LongKey(Long.MAX_VALUE);
		LongKey zero = new LongKey(0);
		LongKey one = new LongKey(1);
		LongKey oneCopy = new LongKey(1);
		LongKey two = new LongKey(4);
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
	}


	@Test
	public void test_getLongIdIfPresent() {
		LongKey longKey = new LongKey(1);
		assertEquals(Long.valueOf(1), longKey.getLongIdIfPresent());	}

	@Test
	public void test_getIntegerIdIfPresent() {
		LongKey longKey = new LongKey(1);
		assertNull(longKey.getIntegerIdIfPresent());
	}

	@Test
	public void test_isInRange() {
		LongKey longKey = new LongKey(1);
		long groupingNumber = longKey.getGroupingNumber();
		assertFalse(longKey.isInRange(groupingNumber - 1, groupingNumber - 1));
		assertTrue(longKey.isInRange(groupingNumber - 1, groupingNumber));
		assertTrue(longKey.isInRange(groupingNumber - 1, groupingNumber + 1));
		assertTrue(longKey.isInRange(groupingNumber, groupingNumber));
		assertTrue(longKey.isInRange(groupingNumber, groupingNumber + 1));
		assertFalse(longKey.isInRange(groupingNumber + 1, groupingNumber + 1));
	}
}
