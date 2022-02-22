package org.bluedb.api.keys;

import java.util.UUID;
import org.junit.Test;
import junit.framework.TestCase;

public class TimeFrameKeyTest extends TestCase {


	@Test
	public void test_constructor_invalid_times() {
		try {
			new TimeFrameKey(Long.MIN_VALUE, 2, 1);
			fail();
		} catch(IllegalArgumentException e) {}
		try {
			new TimeFrameKey("", 2, 1);
			fail();
		} catch(IllegalArgumentException e) {}
		try {
			new TimeFrameKey(UUID.randomUUID(), 2, 1);
			fail();
		} catch(IllegalArgumentException e) {}
		try {
			new TimeFrameKey(new LongKey(1), 2, 1);
			fail();
		} catch(IllegalArgumentException e) {}
	}

	@Test
	public void test_getId() {
		TimeFrameKey min = new TimeFrameKey(Long.MIN_VALUE, 1, 1);
		TimeFrameKey max = new TimeFrameKey(Long.MAX_VALUE, 1, 1);
		TimeFrameKey zero = new TimeFrameKey(0L, 1, 1);
		TimeFrameKey one = new TimeFrameKey(1L, 1, 1);
		assertEquals(new LongKey(Long.MIN_VALUE), min.getId());
		assertEquals(new LongKey(Long.MAX_VALUE), max.getId());
		assertEquals(new LongKey(0), zero.getId());
		assertEquals(new LongKey(1), one.getId());
	}

	@Test
	public void test_getGroupingNumber() {
		TimeFrameKey min = new TimeFrameKey(1, Long.MIN_VALUE, Long.MIN_VALUE);
		TimeFrameKey max = new TimeFrameKey(2, Long.MAX_VALUE, Long.MAX_VALUE);
		TimeFrameKey zero = new TimeFrameKey(3, 0, 0);
		TimeFrameKey one = new TimeFrameKey(4, 1, 1);
		TimeFrameKey oneCopy = new TimeFrameKey(4, 1, 1);
		TimeFrameKey oneDifferent = new TimeFrameKey(5, 1, 1);
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
		TimeFrameKey min = new TimeFrameKey(1, Long.MIN_VALUE, Long.MIN_VALUE);
		TimeFrameKey max = new TimeFrameKey(2, Long.MAX_VALUE, Long.MAX_VALUE);
		TimeFrameKey zero = new TimeFrameKey(3, 0, 0);
		TimeFrameKey one = new TimeFrameKey(4, 1, 1);
		TimeFrameKey oneCopy = new TimeFrameKey(4, 1, 1);
		TimeFrameKey oneDifferent = new TimeFrameKey(5, 1, 1);
		assertEquals(one.hashCode(), oneCopy.hashCode());
		assertTrue(one.hashCode() == oneCopy.hashCode());
		assertFalse(one.hashCode() == oneDifferent.hashCode());
		assertFalse(one.hashCode() == zero.hashCode());
		assertFalse(min.hashCode() == zero.hashCode());
		assertFalse(min.hashCode() == max.hashCode());
		assertFalse(max.hashCode() == zero.hashCode());
		assertFalse(min.hashCode() == zero.hashCode());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void test_equals() {
		TimeFrameKey min = new TimeFrameKey(1, Long.MIN_VALUE, Long.MIN_VALUE);
		TimeFrameKey max = new TimeFrameKey(2, Long.MAX_VALUE, Long.MAX_VALUE);
		TimeFrameKey zero = new TimeFrameKey(3, 0, 0);
		TimeFrameKey one = new TimeFrameKey(4, 1, 1);
		TimeFrameKey oneCopy = new TimeFrameKey(4, 1, 1);
		TimeFrameKey oneDifferent = new TimeFrameKey(5, 1, 1);
		TimeFrameKey oneToTwo = new TimeFrameKey(4, 1, 2);
		TimeFrameKey oneToTwoCopy = new TimeFrameKey(4, 1, 2);
		TimeFrameKey oneToTwoDifferent = new TimeFrameKey(5, 1, 2);
		TimeFrameKey oneToTwoString = new TimeFrameKey("string", 1, 2);
		TimeFrameKey oneToTwoUuid = new TimeFrameKey(new UUID(0, 1), 1, 2);
		StringKey stringKey = new StringKey("1");
		UUIDKey uuidKey = new UUIDKey(UUID.randomUUID());
		assertEquals(one, one);
		assertEquals(one, oneCopy);
		assertTrue(one.equals(oneCopy));
		assertFalse(one.equals(oneDifferent));
		assertFalse(one.equals(oneToTwo));
		assertFalse(oneToTwo.equals(one));
		assertTrue(oneToTwo.equals(oneToTwoCopy));
		assertFalse(oneToTwoDifferent.equals(oneToTwoCopy));
		assertFalse(oneToTwo.equals(oneToTwoDifferent));
		assertFalse(one.equals(zero));
		assertFalse(min.equals(max));
		assertFalse(one.equals(stringKey));
		assertFalse(one.equals(uuidKey));
		assertFalse(one.equals(null));
		assertFalse(one.equals("1"));
		assertFalse(one.equals("1"));
		assertFalse(oneToTwo.equals(oneToTwoString));
		assertFalse(oneToTwo.equals(oneToTwoUuid));
		assertFalse(oneToTwoString.equals(oneToTwo));
		assertFalse(oneToTwoUuid.equals(oneToTwo));
	}

	@Test
	public void test_toString() {
		TimeFrameKey min = new TimeFrameKey(1, Long.MIN_VALUE, Long.MIN_VALUE);
		TimeFrameKey max = new TimeFrameKey(2, Long.MAX_VALUE, Long.MAX_VALUE);
		TimeFrameKey zero = new TimeFrameKey(3, 0, 0);
		TimeFrameKey one = new TimeFrameKey(4, 1, 1);
		TimeFrameKey oneToTwoCopy = new TimeFrameKey(4, 1, 2);
		assertTrue(min.toString().contains(String.valueOf(min.getId())));
		assertTrue(max.toString().contains(String.valueOf(max.getId())));
		assertTrue(zero.toString().contains(String.valueOf(zero)));
		assertTrue(one.toString().contains(String.valueOf(one.getId())));
		assertTrue(oneToTwoCopy.toString().contains(String.valueOf(oneToTwoCopy.getId())));
		assertTrue(oneToTwoCopy.toString().contains(String.valueOf(oneToTwoCopy.getStartTime())));
		assertTrue(oneToTwoCopy.toString().contains(String.valueOf(oneToTwoCopy.getEndTime())));

		assertTrue(min.toString().contains(min.getClass().getSimpleName()));
}

	@Test
	public void test_compareTo() {
		TimeFrameKey min = new TimeFrameKey(1, Long.MIN_VALUE, Long.MIN_VALUE);
		TimeFrameKey max = new TimeFrameKey(2, Long.MAX_VALUE, Long.MAX_VALUE);
		TimeFrameKey zero = new TimeFrameKey(3, 0, 0);
		TimeFrameKey one = new TimeFrameKey(4, 1, 1);
		TimeFrameKey oneCopy = new TimeFrameKey(4, 1, 1);
		TimeFrameKey oneDifferent = new TimeFrameKey(5, 1, 1);
		TimeFrameKey oneToTwo = new TimeFrameKey(4, 1, 2);
		TimeFrameKey oneToTwoCopy = new TimeFrameKey(4, 1, 2);
		StringKey stringKey = new StringKey("1");
		UUIDKey uuidKey = new UUIDKey(UUID.randomUUID());
		assertTrue(zero.compareTo(one) < 0);  // basic
		assertTrue(one.compareTo(zero) > 0);  // reverse
		assertTrue(min.compareTo(max) < 0);  // extreme
		assertTrue(max.compareTo(min) > 0);  // extreme backwards
		assertTrue(one.compareTo(oneCopy) == 0);  // equals
		assertTrue(oneToTwo.compareTo(oneToTwoCopy) == 0);  // equals
		assertTrue(one.compareTo(oneToTwo) < 0);  // same start but different end
		assertTrue(one.compareTo(oneDifferent) != 0);  // same time but not equals
		assertTrue(one.compareTo(null) != 0);  // sanity check
		assertTrue(one.compareTo(stringKey) != 0);  // sanity check
		assertTrue(one.compareTo(uuidKey) != 0);  // sanity check
	}

	@Test
	public void test_compareClass() {
		TimeFrameKey one = new TimeFrameKey(4, 1, 1);
		StringKey stringKey = new StringKey("1");
		assertTrue(one.compareCanonicalClassNames(stringKey) != 0);
		assertTrue(one.compareCanonicalClassNames(null) == -1);
		assertEquals(one.compareCanonicalClassNames(stringKey), -stringKey.compareCanonicalClassNames(one));
	}

	@Test
	public void test_isBeforeRange() {
		TimeFrameKey _2_to_4 = new TimeFrameKey(1, 2, 4);
		assertFalse(_2_to_4.isBeforeRange(0, 1));
		assertFalse(_2_to_4.isBeforeRange(0, 2));
		assertFalse(_2_to_4.isBeforeRange(0, 3));
		assertFalse(_2_to_4.isBeforeRange(3, 3));
		assertFalse(_2_to_4.isBeforeRange(0, 6));
		assertFalse(_2_to_4.isBeforeRange(3, 6));
		assertFalse(_2_to_4.isBeforeRange(4, 6));
		assertTrue(_2_to_4.isBeforeRange(5, 6));
	}

	@Test
	public void test_isInRange() {
		TimeFrameKey _2_to_4 = new TimeFrameKey(1, 2, 4);
		assertFalse(_2_to_4.isInRange(0, 1));
		assertTrue(_2_to_4.isInRange(0, 2));
		assertTrue(_2_to_4.isInRange(0, 3));
		assertTrue(_2_to_4.isInRange(3, 3));
		assertTrue(_2_to_4.isInRange(0, 6));
		assertTrue(_2_to_4.isInRange(3, 6));
		assertTrue(_2_to_4.isInRange(4, 6));
		assertFalse(_2_to_4.isInRange(5, 6));
	}

	@Test
	public void test_isAfterRange() {
		TimeFrameKey _2_to_4 = new TimeFrameKey(1, 2, 4);
		assertTrue(_2_to_4.isAfterRange(0, 1));
		assertFalse(_2_to_4.isAfterRange(0, 2));
		assertFalse(_2_to_4.isAfterRange(0, 3));
		assertFalse(_2_to_4.isAfterRange(3, 3));
		assertFalse(_2_to_4.isAfterRange(0, 6));
		assertFalse(_2_to_4.isAfterRange(3, 6));
		assertFalse(_2_to_4.isAfterRange(4, 6));
		assertFalse(_2_to_4.isAfterRange(5, 6));
	}
}
