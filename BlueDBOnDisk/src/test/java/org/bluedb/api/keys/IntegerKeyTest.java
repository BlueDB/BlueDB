package org.bluedb.api.keys;

import java.util.UUID;
import org.junit.Test;
import junit.framework.TestCase;

public class IntegerKeyTest extends TestCase {

	@Test
	public void test_getId() {
		IntegerKey min = new IntegerKey(Integer.MIN_VALUE);
		IntegerKey max = new IntegerKey(Integer.MAX_VALUE);
		IntegerKey zero = new IntegerKey(0);
		IntegerKey one = new IntegerKey(1);
		assertEquals(Integer.MIN_VALUE, min.getId());
		assertEquals(Integer.MAX_VALUE, max.getId());
		assertEquals(0, zero.getId());
		assertEquals(1, one.getId());
	}

	@Test
	public void test_getGroupingNumber() {
		IntegerKey min = new IntegerKey(Integer.MIN_VALUE);
		IntegerKey max = new IntegerKey(Integer.MAX_VALUE);
		IntegerKey zero = new IntegerKey(0);
		IntegerKey one = new IntegerKey(1);
		IntegerKey oneCopy = new IntegerKey(1);
		assertEquals(one.getGroupingNumber(), oneCopy.getGroupingNumber());
		assertTrue(one.getGroupingNumber() == oneCopy.getGroupingNumber());
		assertFalse(one.getGroupingNumber() == zero.getGroupingNumber());
		assertFalse(min.getGroupingNumber() == max.getGroupingNumber());

		assertTrue(min.getGroupingNumber() >= 0);
		assertTrue(max.getGroupingNumber() >= 0);
		assertTrue(zero.getGroupingNumber() >= 0);
		assertTrue(one.getGroupingNumber() >= 0);

		long integerMaxValueAsLong = Integer.MAX_VALUE;
		assertTrue(min.getGroupingNumber() <= integerMaxValueAsLong * 2 + 1);
		assertTrue(max.getGroupingNumber() <= integerMaxValueAsLong * 2 + 1);
		assertTrue(zero.getGroupingNumber() <= integerMaxValueAsLong * 2 + 1);
		assertTrue(one.getGroupingNumber() <= integerMaxValueAsLong * 2 + 1);
	}

	@Test
	public void test_hashCode() {
		IntegerKey min = new IntegerKey(Integer.MIN_VALUE);
		IntegerKey max = new IntegerKey(Integer.MAX_VALUE);
		IntegerKey zero = new IntegerKey(0);
		IntegerKey one = new IntegerKey(1);
		IntegerKey oneCopy = new IntegerKey(1);
		assertEquals(one.hashCode(), oneCopy.hashCode());
		assertTrue(one.hashCode() == oneCopy.hashCode());
		assertFalse(one.hashCode() == zero.hashCode());
		assertFalse(min.hashCode() == zero.hashCode());
		assertFalse(min.hashCode() == max.hashCode());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void test_equals() {
		IntegerKey min = new IntegerKey(Integer.MIN_VALUE);
		IntegerKey max = new IntegerKey(Integer.MAX_VALUE);
		IntegerKey zero = new IntegerKey(0);
		IntegerKey one = new IntegerKey(1);
		IntegerKey oneCopy = new IntegerKey(1);
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
		IntegerKey min = new IntegerKey(Integer.MIN_VALUE);
		IntegerKey max = new IntegerKey(Integer.MAX_VALUE);
		IntegerKey zero = new IntegerKey(0);
		IntegerKey one = new IntegerKey(1);
		assertTrue(min.toString().contains(String.valueOf(min.getId())));
		assertTrue(max.toString().contains(String.valueOf(max.getId())));
		assertTrue(zero.toString().contains(String.valueOf(zero)));
		assertTrue(one.toString().contains(String.valueOf(one.getId())));

		assertTrue(min.toString().contains(min.getClass().getSimpleName()));
}

	@Test
	public void test_compareTo() {
		IntegerKey min = new IntegerKey(Integer.MIN_VALUE);
		IntegerKey max = new IntegerKey(Integer.MAX_VALUE);
		IntegerKey zero = new IntegerKey(0);
		IntegerKey one = new IntegerKey(1);
		IntegerKey oneCopy = new IntegerKey(1);
		IntegerKey two = new IntegerKey(4);
		StringKey stringKey = new StringKey("1");
		UUIDKey uuidKey = new UUIDKey(UUID.randomUUID());
		TimeKey timeKeyWithGroupingNumberMatchingTwo = new TimeKey(5, 2147483652l);
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
		IntegerKey integerKey = new IntegerKey(1);
		assertNull(integerKey.getLongIdIfPresent());
	}

	@Test
	public void test_getIntegerIdIfPresent() {
		IntegerKey integerKey = new IntegerKey(1);
		assertEquals(Integer.valueOf(1), integerKey.getIntegerIdIfPresent());
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
