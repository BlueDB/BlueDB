package io.bluedb.api.keys;

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
		assertFalse(one.getGroupingNumber() == zero.getGroupingNumber());
//		assertFalse(min.getGroupingNumber() == max.getGroupingNumber());  // Actually, true.  In fact, Long.hashCode(Long.MAX_VALUE) == Long.hashCode(Long.MIN_VALUE)
		assertFalse(max.getGroupingNumber() == zero.getGroupingNumber());
		assertFalse(min.getGroupingNumber() == zero.getGroupingNumber());
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
		StringKey stringKey = new StringKey("1");
		assertTrue(zero.compareTo(one) < 0);  // basic
		assertTrue(one.compareTo(zero) > 0);  // reverse
		assertTrue(min.compareTo(max) < 0);  // extreme
		assertTrue(max.compareTo(min) > 0);  // extreme backwards
		assertTrue(one.compareTo(oneCopy) == 0);  // equals
		assertTrue(one.compareTo(null) != 0);  // sanity check
		assertTrue(one.compareTo(stringKey) != 0);  // sanity check
	}
}
