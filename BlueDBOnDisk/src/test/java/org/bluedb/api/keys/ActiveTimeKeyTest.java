package org.bluedb.api.keys;

import java.util.UUID;
import org.junit.Test;
import junit.framework.TestCase;

public class ActiveTimeKeyTest extends TestCase {

	@Test
	public void test_getId() {
		ActiveTimeKey min = new ActiveTimeKey(Long.MIN_VALUE, 1);
		ActiveTimeKey max = new ActiveTimeKey(Long.MAX_VALUE, 1);
		ActiveTimeKey zero = new ActiveTimeKey(0L, 1);
		ActiveTimeKey one = new ActiveTimeKey(1L, 1);
		assertEquals(new LongKey(Long.MIN_VALUE), min.getId());
		assertEquals(new LongKey(Long.MAX_VALUE), max.getId());
		assertEquals(new LongKey(0), zero.getId());
		assertEquals(new LongKey(1), one.getId());
	}

	@Test
	public void test_getGroupingNumber() {
		ActiveTimeKey min = new ActiveTimeKey(1, Long.MIN_VALUE);
		ActiveTimeKey max = new ActiveTimeKey(2, Long.MAX_VALUE);
		ActiveTimeKey zero = new ActiveTimeKey(3, 0);
		ActiveTimeKey one = new ActiveTimeKey(4, 1);
		ActiveTimeKey oneCopy = new ActiveTimeKey(4, 1);
		ActiveTimeKey oneDifferent = new ActiveTimeKey(4, 1);
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
		ActiveTimeKey min = new ActiveTimeKey(1, Long.MIN_VALUE);
		ActiveTimeKey max = new ActiveTimeKey(2, Long.MAX_VALUE);
		ActiveTimeKey zero = new ActiveTimeKey(3, 0);
		ActiveTimeKey one = new ActiveTimeKey(4, 1);
		ActiveTimeKey oneCopy = new ActiveTimeKey(4, 1);
		ActiveTimeKey oneDifferent = new ActiveTimeKey(5, 1);
		ValueKey _null = null;
		ActiveTimeKey nullTime10 = new ActiveTimeKey(_null, 10L);
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
		ActiveTimeKey min = new ActiveTimeKey(1, Long.MIN_VALUE);
		ActiveTimeKey max = new ActiveTimeKey(2, Long.MAX_VALUE);
		ActiveTimeKey zero = new ActiveTimeKey(3, 0);
		ActiveTimeKey one = new ActiveTimeKey(4, 1);
		ActiveTimeKey oneCopy = new ActiveTimeKey(4, 1);
		ActiveTimeKey oneDifferent = new ActiveTimeKey(5, 1);
		ValueKey _null = null;
		ActiveTimeKey nullTime10 = new ActiveTimeKey(_null, 10L);
		ActiveTimeKey nullTime10copy = new ActiveTimeKey(_null, 10L);
		ActiveTimeKey nullTime11 = new ActiveTimeKey(_null, 11L);
		StringKey stringKey = new StringKey("1");
		UUIDKey uuidKey = new UUIDKey(UUID.randomUUID());
		ActiveTimeKey stringActiveTimeKey = new ActiveTimeKey("1", 1L);
		ActiveTimeKey stringActiveTimeKeyCopy = new ActiveTimeKey("1", 1L);
		assertEquals(one, one);
		assertEquals(one, oneCopy);
		assertTrue(one.equals(oneCopy));
		assertFalse(one.equals(oneDifferent));
		assertFalse(one.equals(zero));
		assertFalse(min.equals(max));
		assertFalse(one.equals(stringKey));
		assertFalse(one.equals(uuidKey));
		assertFalse(one.equals(nullTime10));
		assertFalse(one.equals(stringActiveTimeKeyCopy));
		assertTrue(stringActiveTimeKey.equals(stringActiveTimeKeyCopy));
		assertTrue(stringActiveTimeKeyCopy.equals(stringActiveTimeKey));
		assertFalse(nullTime10.equals(one));
		assertFalse(nullTime11.equals(nullTime10));
		assertFalse(nullTime10.equals(nullTime11));
		assertTrue(nullTime10.equals(nullTime10copy));
		assertFalse(one.equals(null));
		assertFalse(one.equals("1"));
	}

	@Test
	public void test_toString() {
		ActiveTimeKey min = new ActiveTimeKey(1, Long.MIN_VALUE);
		ActiveTimeKey max = new ActiveTimeKey(2, Long.MAX_VALUE);
		ActiveTimeKey zero = new ActiveTimeKey(3, 0);
		ActiveTimeKey one = new ActiveTimeKey(4, 1);
		assertTrue(min.toString().contains(String.valueOf(min.getId())));
		assertTrue(max.toString().contains(String.valueOf(max.getId())));
		assertTrue(zero.toString().contains(String.valueOf(zero)));
		assertTrue(one.toString().contains(String.valueOf(one.getId())));

		assertTrue(min.toString().contains(min.getClass().getSimpleName()));
}

	@Test
	public void test_compareTo() {
		ActiveTimeKey min = new ActiveTimeKey(1, Long.MIN_VALUE);
		ActiveTimeKey max = new ActiveTimeKey(2, Long.MAX_VALUE);
		ActiveTimeKey zero = new ActiveTimeKey(3, 0);
		ActiveTimeKey one = new ActiveTimeKey(4, 1);
		ActiveTimeKey oneCopy = new ActiveTimeKey(4, 1);
		ActiveTimeKey oneDifferent = new ActiveTimeKey(5, 1);
		StringKey stringKey = new StringKey("1");
		UUIDKey uuidKey = new UUIDKey(UUID.randomUUID());
		LongKey longKey = new LongKey(4);
		ActiveTimeKey activeTimeKeyWithGroupingNumberMatchingLong = new ActiveTimeKey(5, 4611686018427387906l);
		assertTrue(zero.compareTo(one) < 0);  // basic
		assertTrue(one.compareTo(zero) > 0);  // reverse
		assertTrue(min.compareTo(max) < 0);  // extreme
		assertTrue(max.compareTo(min) > 0);  // extreme backwards
		assertTrue(one.compareTo(oneCopy) == 0);  // equals
		assertTrue(one.compareTo(oneDifferent) != 0);  // same time but not equals
		assertTrue(one.compareTo(null) != 0);  // sanity check
		assertTrue(one.compareTo(stringKey) != 0);  // sanity check
		assertTrue(one.compareTo(uuidKey) != 0);  // sanity check
		assertTrue(activeTimeKeyWithGroupingNumberMatchingLong.compareTo(longKey) < 0);  // same grouping number, different class
	}

	@Test
	public void test_getLongIdIfPresent() {
		ActiveTimeKey fourLongAtOne = new ActiveTimeKey(4L, 1);
		ActiveTimeKey fourIntegerAtOne = new ActiveTimeKey(new IntegerKey(4), 1);
		assertEquals(Long.valueOf(4L), fourLongAtOne.getLongIdIfPresent());
		assertNull(fourIntegerAtOne.getLongIdIfPresent());
	}

	@Test
	public void test_getIntegerIdIfPresent() {
		ActiveTimeKey fourLongAtOne = new ActiveTimeKey(4L, 1);
		ActiveTimeKey fourIntegerAtOne = new ActiveTimeKey(new IntegerKey(4), 1);
		assertNull(fourLongAtOne.getIntegerIdIfPresent());
		assertEquals(Integer.valueOf(4), fourIntegerAtOne.getIntegerIdIfPresent());
	}

	@Test
	public void test_getStringIdIfPresent() {
		ActiveTimeKey uuidInActiveTimeKey = new ActiveTimeKey(UUID.randomUUID(), 1);
		ActiveTimeKey stringInActiveTimeKey = new ActiveTimeKey("whatever", 1);
		assertNull(uuidInActiveTimeKey.getStringIdIfPresent());
		assertEquals("whatever", stringInActiveTimeKey.getStringIdIfPresent());
	}

	@Test
	public void test_getUUIDIdIfPresent() {
		UUID uuid = UUID.randomUUID();
		ActiveTimeKey uuidInActiveTimeKey = new ActiveTimeKey(uuid, 1);
		ActiveTimeKey stringInActiveTimeKey = new ActiveTimeKey("whatever", 1);
		assertNull(stringInActiveTimeKey.getUUIDIdIfPresent());
		assertEquals(uuid, uuidInActiveTimeKey.getUUIDIdIfPresent());
	}

	@Test
	public void test_isInRange() {
		BlueKey _4 = new ActiveTimeKey(4, 4);
		assertFalse(_4.overlapsRange(0, 3));
		assertTrue(_4.overlapsRange(0, 4));
		assertTrue(_4.overlapsRange(0, 6));
		assertTrue(_4.overlapsRange(4, 6));
		assertTrue(_4.overlapsRange(5, 6));
	}

	@Test
	public void test_isAfterRange() {
		BlueKey _4 = new ActiveTimeKey(4, 4);
		assertTrue(_4.isAfterRange(0, 3));
		assertFalse(_4.isAfterRange(0, 4));
		assertFalse(_4.isAfterRange(0, 6));
		assertFalse(_4.isAfterRange(4, 6));
		assertFalse(_4.isAfterRange(5, 6));
	}
	
	@Test
	public void test_isActiveTimeKey() {
		ActiveTimeKey _4 = new ActiveTimeKey(4, 4);
		assertTrue(_4.isActiveTimeKey());
	}
}
