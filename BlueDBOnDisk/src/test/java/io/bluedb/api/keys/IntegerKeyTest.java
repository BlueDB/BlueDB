package io.bluedb.api.keys;

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
		assertEquals(one, one);
		assertEquals(one, oneCopy);
		assertTrue(one.equals(oneCopy));
		assertFalse(one.equals(zero));
		assertFalse(min.equals(max));
		assertFalse(one.equals(stringKey));
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
		StringKey stringKey = new StringKey("1");
		assertTrue(zero.compareTo(one) < 0);  // basic
		assertTrue(one.compareTo(zero) > 0);  // reverse
		assertTrue(min.compareTo(max) < 0);  // extreme
		assertTrue(max.compareTo(min) > 0);  // extreme backwards
		assertTrue(one.compareTo(oneCopy) == 0);  // equals
		assertTrue(one.compareTo(null) != 0);  // sanity check
		assertTrue(one.compareTo(stringKey) != 0);  // sanity check
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
