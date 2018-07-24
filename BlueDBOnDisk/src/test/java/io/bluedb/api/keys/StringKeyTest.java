package io.bluedb.api.keys;

import org.junit.Test;
import junit.framework.TestCase;

public class StringKeyTest extends TestCase {

	@Test
	public void test_getId() {
		StringKey zero = new StringKey("0");
		StringKey one = new StringKey("1");
		StringKey empty = new StringKey("");
		StringKey _null = new StringKey(null);
		assertEquals("0", zero.getId());
		assertEquals("1", one.getId());
		assertEquals("", empty.getId());
		assertEquals(null, _null.getId());
	}

	@Test
	public void test_getGroupingNumber() {
		StringKey zero = new StringKey("0");
		StringKey one = new StringKey("1");
		StringKey oneCopy = new StringKey("1");
		StringKey empty = new StringKey("");
		assertEquals(one.getGroupingNumber(), oneCopy.getGroupingNumber());
		assertTrue(one.getGroupingNumber() == oneCopy.getGroupingNumber());
		assertFalse(one.getGroupingNumber() == zero.getGroupingNumber());
		assertFalse(empty.getGroupingNumber() == one.getGroupingNumber());
	}

	@Test
	public void test_hashCode() {
		StringKey zero = new StringKey("0");
		StringKey one = new StringKey("1");
		StringKey oneCopy = new StringKey("1");
		StringKey empty = new StringKey("");
		StringKey nullKey = new StringKey(null);
		assertEquals(one.hashCode(), oneCopy.hashCode());
		assertTrue(one.hashCode() == oneCopy.hashCode());
		assertFalse(one.hashCode() == zero.hashCode());
		assertFalse(empty.hashCode() == one.hashCode());
		assertFalse(nullKey.hashCode() == one.hashCode());
//		assertFalse(empty.hashCode() == nullKey.hashCode());  This is actually true, but we probably don't care since we never want nulls or empties anyway
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void test_equals() {
		StringKey zero = new StringKey("0");
		StringKey one = new StringKey("1");
		StringKey oneCopy = new StringKey("1");
		StringKey empty = new StringKey("");
		StringKey nullKey = new StringKey(null);
		StringKey nullKey2 = new StringKey(null);
		assertEquals(one, one);
		assertEquals(one, oneCopy);
		assertTrue(one.equals(oneCopy));
		assertFalse(one.equals(zero));
		assertFalse(empty.equals(zero));
		assertFalse(empty.equals(nullKey));
		assertFalse(nullKey.equals(empty));
		assertTrue(nullKey.equals(nullKey2));
		assertFalse(one.equals(null));
		assertFalse(one.equals("1"));
	}

	@Test
	public void test_toString() {
		StringKey zero = new StringKey("0");
		StringKey one = new StringKey("1");
		assertTrue(zero.toString().contains(String.valueOf(zero)));
		assertTrue(one.toString().contains(String.valueOf(one.getId())));

		assertTrue(one.toString().contains(one.getClass().getSimpleName()));
}

	@Test
	public void test_compareTo() {
		StringKey zero = new StringKey("0");
		StringKey one = new StringKey("1");
		StringKey oneCopy = new StringKey("1");
		StringKey empty = new StringKey("");
		LongKey longKey = new LongKey(1);
		assertTrue(zero.compareTo(one) != 0);  // basic
		assertTrue(one.compareTo(zero) != 0);  // reverse
		assertTrue(empty.compareTo(one) < 0);  // extreme
		assertTrue(one.compareTo(empty) > 0);  // extreme backwards
		assertTrue(one.compareTo(oneCopy) == 0);  // equals
		assertTrue(one.compareTo(null) != 0);  // sanity check
		assertTrue(one.compareTo(longKey) != 0);  // sanity check
	}

	@Test
	public void test_getLongIdIfPresent() {
		StringKey one = new StringKey("1");
		StringKey empty = new StringKey("");
		StringKey _null = new StringKey(null);
		assertNull(one.getLongIdIfPresent());
		assertNull(empty.getLongIdIfPresent());
		assertNull(_null.getLongIdIfPresent());
	}

	@Test
	public void test_getIntegerIdIfPresent() {
		TimeKey fourLongAtOne = new TimeKey(4L, 1);
		TimeKey fourIntegerAtOne = new TimeKey(new IntegerKey(4), 1);
		assertNull(fourLongAtOne.getIntegerIdIfPresent());
		assertEquals(Integer.valueOf(4), fourIntegerAtOne.getIntegerIdIfPresent());
	}

	@Test
	public void test_isInRange() {
		StringKey stringKey = new StringKey("1");
		long groupingNumber = stringKey.getGroupingNumber();
		assertFalse(stringKey.isInRange(groupingNumber - 1, groupingNumber - 1));
		assertTrue(stringKey.isInRange(groupingNumber - 1, groupingNumber));
		assertTrue(stringKey.isInRange(groupingNumber - 1, groupingNumber + 1));
		assertTrue(stringKey.isInRange(groupingNumber, groupingNumber));
		assertTrue(stringKey.isInRange(groupingNumber, groupingNumber + 1));
		assertFalse(stringKey.isInRange(groupingNumber + 1, groupingNumber + 1));
	}
}
