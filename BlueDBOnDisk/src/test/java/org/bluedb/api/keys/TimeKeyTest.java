package org.bluedb.api.keys;

import java.util.UUID;
import org.junit.Test;
import junit.framework.TestCase;

public class TimeKeyTest extends TestCase {

	@Test
	public void test_getId() {
		TimeKey min = new TimeKey(Long.MIN_VALUE, 1);
		TimeKey max = new TimeKey(Long.MAX_VALUE, 1);
		TimeKey zero = new TimeKey(0L, 1);
		TimeKey one = new TimeKey(1L, 1);
		assertEquals(new LongKey(Long.MIN_VALUE), min.getId());
		assertEquals(new LongKey(Long.MAX_VALUE), max.getId());
		assertEquals(new LongKey(0), zero.getId());
		assertEquals(new LongKey(1), one.getId());
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
		TimeKey min = new TimeKey(1, Long.MIN_VALUE);
		TimeKey max = new TimeKey(2, Long.MAX_VALUE);
		TimeKey zero = new TimeKey(3, 0);
		TimeKey one = new TimeKey(4, 1);
		TimeKey oneCopy = new TimeKey(4, 1);
		TimeKey oneDifferent = new TimeKey(5, 1);
		ValueKey _null = null;
		TimeKey nullTime10 = new TimeKey(_null, 10L);
		assertEquals(one.hashCode(), oneCopy.hashCode());
		assertTrue(one.hashCode() == oneCopy.hashCode());
		assertFalse(one.hashCode() == oneDifferent.hashCode());
		assertFalse(one.hashCode() == zero.hashCode());
		assertFalse(min.hashCode() == zero.hashCode());
		assertFalse(min.hashCode() == max.hashCode());
		assertFalse(max.hashCode() == zero.hashCode());
		assertFalse(min.hashCode() == zero.hashCode());
		assertFalse(nullTime10.hashCode() == zero.hashCode());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void test_equals() {
		TimeKey min = new TimeKey(1, Long.MIN_VALUE);
		TimeKey max = new TimeKey(2, Long.MAX_VALUE);
		TimeKey zero = new TimeKey(3, 0);
		TimeKey one = new TimeKey(4, 1);
		TimeKey oneCopy = new TimeKey(4, 1);
		TimeKey oneDifferent = new TimeKey(5, 1);
		ValueKey _null = null;
		TimeKey nullTime10 = new TimeKey(_null, 10L);
		TimeKey nullTime10copy = new TimeKey(_null, 10L);
		TimeKey nullTime11 = new TimeKey(_null, 11L);
		StringKey stringKey = new StringKey("1");
		UUIDKey uuidKey = new UUIDKey(UUID.randomUUID());
		TimeKey stringTimeKey = new TimeKey("1", 1L);
		TimeKey stringTimeKeyCopy = new TimeKey("1", 1L);
		assertEquals(one, one);
		assertEquals(one, oneCopy);
		assertTrue(one.equals(oneCopy));
		assertFalse(one.equals(oneDifferent));
		assertFalse(one.equals(zero));
		assertFalse(min.equals(max));
		assertFalse(one.equals(stringKey));
		assertFalse(one.equals(uuidKey));
		assertFalse(one.equals(nullTime10));
		assertFalse(one.equals(stringTimeKeyCopy));
		assertTrue(stringTimeKey.equals(stringTimeKeyCopy));
		assertTrue(stringTimeKeyCopy.equals(stringTimeKey));
		assertFalse(nullTime10.equals(one));
		assertFalse(nullTime11.equals(nullTime10));
		assertFalse(nullTime10.equals(nullTime11));
		assertTrue(nullTime10.equals(nullTime10copy));
		assertFalse(one.equals(null));
		assertFalse(one.equals("1"));
	}

	@Test
	public void test_toString() {
		TimeKey min = new TimeKey(1, Long.MIN_VALUE);
		TimeKey max = new TimeKey(2, Long.MAX_VALUE);
		TimeKey zero = new TimeKey(3, 0);
		TimeKey one = new TimeKey(4, 1);
		assertTrue(min.toString().contains(String.valueOf(min.getId())));
		assertTrue(max.toString().contains(String.valueOf(max.getId())));
		assertTrue(zero.toString().contains(String.valueOf(zero)));
		assertTrue(one.toString().contains(String.valueOf(one.getId())));

		assertTrue(min.toString().contains(min.getClass().getSimpleName()));
}

	@Test
	public void test_compareTo() {
		TimeKey min = new TimeKey(1, Long.MIN_VALUE);
		TimeKey max = new TimeKey(2, Long.MAX_VALUE);
		TimeKey zero = new TimeKey(3, 0);
		TimeKey one = new TimeKey(4, 1);
		TimeKey oneCopy = new TimeKey(4, 1);
		TimeKey oneDifferent = new TimeKey(5, 1);
		StringKey stringKey = new StringKey("1");
		UUIDKey uuidKey = new UUIDKey(UUID.randomUUID());
		LongKey longKey = new LongKey(4);
		TimeKey timeKeyWithGroupingNumberMatchingLong = new TimeKey(5, 4611686018427387906l);
		assertTrue(zero.compareTo(one) < 0);  // basic
		assertTrue(one.compareTo(zero) > 0);  // reverse
		assertTrue(min.compareTo(max) < 0);  // extreme
		assertTrue(max.compareTo(min) > 0);  // extreme backwards
		assertTrue(one.compareTo(oneCopy) == 0);  // equals
		assertTrue(one.compareTo(oneDifferent) != 0);  // same time but not equals
		assertTrue(one.compareTo(null) != 0);  // sanity check
		assertTrue(one.compareTo(stringKey) != 0);  // sanity check
		assertTrue(one.compareTo(uuidKey) != 0);  // sanity check
		assertTrue(timeKeyWithGroupingNumberMatchingLong.compareTo(longKey) > 0);  // same grouping number, different class
	}

	@Test
	public void test_getLongIdIfPresent() {
		TimeKey fourLongAtOne = new TimeKey(4L, 1);
		TimeKey fourIntegerAtOne = new TimeKey(new IntegerKey(4), 1);
		assertEquals(Long.valueOf(4L), fourLongAtOne.getLongIdIfPresent());
		assertNull(fourIntegerAtOne.getLongIdIfPresent());
	}

	@Test
	public void test_getIntegerIdIfPresent() {
		TimeKey fourLongAtOne = new TimeKey(4L, 1);
		TimeKey fourIntegerAtOne = new TimeKey(new IntegerKey(4), 1);
		assertNull(fourLongAtOne.getIntegerIdIfPresent());
		assertEquals(Integer.valueOf(4), fourIntegerAtOne.getIntegerIdIfPresent());
	}

	@Test
	public void test_isBeforeRange() {
		BlueKey _4 = new TimeKey(4, 4);
		assertFalse(_4.isBeforeRange(0, 3));
		assertFalse(_4.isBeforeRange(0, 4));
		assertFalse(_4.isBeforeRange(0, 6));
		assertFalse(_4.isBeforeRange(4, 6));
		assertTrue(_4.isBeforeRange(5, 6));
	}

	@Test
	public void test_isInRange() {
		BlueKey _4 = new TimeKey(4, 4);
		assertFalse(_4.isInRange(0, 3));
		assertTrue(_4.isInRange(0, 4));
		assertTrue(_4.isInRange(0, 6));
		assertTrue(_4.isInRange(4, 6));
		assertFalse(_4.isInRange(5, 6));
	}

	@Test
	public void test_isAfterRange() {
		BlueKey _4 = new TimeKey(4, 4);
		assertTrue(_4.isAfterRange(0, 3));
		assertFalse(_4.isAfterRange(0, 4));
		assertFalse(_4.isAfterRange(0, 6));
		assertFalse(_4.isAfterRange(4, 6));
		assertFalse(_4.isAfterRange(5, 6));
	}
}
