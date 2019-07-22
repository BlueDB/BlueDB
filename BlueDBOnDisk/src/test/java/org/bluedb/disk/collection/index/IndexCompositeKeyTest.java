package org.bluedb.disk.collection.index;

import static org.junit.Assert.*;
import org.junit.Test;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;

public class IndexCompositeKeyTest {

	@Test
	public void test_getValueKey() {
		TimeFrameKey timeFrameKey = new TimeFrameKey(1, 2, 3);
		LongKey longKey = new LongKey(4L);
		IndexCompositeKey<TimeFrameKey> compositeKeyTime = new IndexCompositeKey<TimeFrameKey>(timeFrameKey, longKey);
		assertEquals(compositeKeyTime.getValueKey(), longKey);
	}

	@Test
	public void test_getGroupingNumber() {
		TimeFrameKey timeFrameKey = new TimeFrameKey(1, 2, 3);
		LongKey longKey = new LongKey(4L);
		IndexCompositeKey<TimeFrameKey> compositeKeyTime = new IndexCompositeKey<TimeFrameKey>(timeFrameKey, longKey);
		IndexCompositeKey<LongKey> compositeKeyLong = new IndexCompositeKey<LongKey>(longKey, timeFrameKey);
		assertEquals(timeFrameKey.getGroupingNumber(), compositeKeyTime.getGroupingNumber());
		assertEquals(longKey.getGroupingNumber(), compositeKeyLong.getGroupingNumber());
	}

	@Test
	public void test_hashCode() {
		TimeFrameKey timeFrameKey = new TimeFrameKey(1, 2, 3);
		TimeFrameKey timeFrameKeyCopy = new TimeFrameKey(1, 2, 3);
		LongKey longKey = new LongKey(4L);
		LongKey longKeyCopy = new LongKey(4L);

		IndexCompositeKey<TimeFrameKey> compositeKeyTimeLong = new IndexCompositeKey<TimeFrameKey>(timeFrameKey, longKey);
		IndexCompositeKey<TimeFrameKey> compositeKeyTimeLongCopy = new IndexCompositeKey<TimeFrameKey>(timeFrameKeyCopy, longKeyCopy);
		IndexCompositeKey<TimeFrameKey> compositeKeyTimeTime = new IndexCompositeKey<TimeFrameKey>(timeFrameKey, timeFrameKey);
		IndexCompositeKey<LongKey> compositeKeyLongLong = new IndexCompositeKey<LongKey>(longKey, longKey);
		IndexCompositeKey<TimeFrameKey> compositeKeyTimeNull = new IndexCompositeKey<TimeFrameKey>(timeFrameKey, null);
		IndexCompositeKey<TimeFrameKey> compositeKeyNullLong = new IndexCompositeKey<TimeFrameKey>(null, longKey);

		assertTrue(compositeKeyTimeLong.hashCode() == compositeKeyTimeLongCopy.hashCode());
		assertFalse(compositeKeyTimeLong.hashCode() == compositeKeyTimeTime.hashCode());
		assertFalse(compositeKeyTimeLong.hashCode() == compositeKeyLongLong.hashCode());
		assertFalse(compositeKeyTimeLong.hashCode() == compositeKeyTimeNull.hashCode());
		assertFalse(compositeKeyTimeLong.hashCode() == compositeKeyNullLong.hashCode());

		assertTrue(compositeKeyTimeLongCopy.hashCode() == compositeKeyTimeLong.hashCode());
		assertFalse(compositeKeyTimeTime.hashCode() == compositeKeyTimeLong.hashCode());
		assertFalse(compositeKeyLongLong.hashCode() == compositeKeyTimeLong.hashCode());
		assertFalse(compositeKeyTimeNull.hashCode() == compositeKeyTimeLong.hashCode());
		assertFalse(compositeKeyNullLong.hashCode() == compositeKeyTimeLong.hashCode());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void test_equals() {
		TimeFrameKey timeFrameKey = new TimeFrameKey(1, 2, 3);
		TimeFrameKey timeFrameKeyCopy = new TimeFrameKey(1, 2, 3);
		LongKey longKey = new LongKey(4L);
		LongKey longKeyCopy = new LongKey(4L);

		IndexCompositeKey<TimeFrameKey> compositeKeyTimeLong = new IndexCompositeKey<TimeFrameKey>(timeFrameKey, longKey);
		IndexCompositeKey<TimeFrameKey> compositeKeyTimeLongCopy = new IndexCompositeKey<TimeFrameKey>(timeFrameKeyCopy, longKeyCopy);
		IndexCompositeKey<TimeFrameKey> compositeKeyTimeTime = new IndexCompositeKey<TimeFrameKey>(timeFrameKey, timeFrameKey);
		IndexCompositeKey<LongKey> compositeKeyLongLong = new IndexCompositeKey<LongKey>(longKey, longKey);
		IndexCompositeKey<TimeFrameKey> compositeKeyTimeNull = new IndexCompositeKey<TimeFrameKey>(timeFrameKey, null);
		IndexCompositeKey<TimeFrameKey> compositeKeyNullLong = new IndexCompositeKey<TimeFrameKey>(null, longKey);

		assertTrue(compositeKeyTimeLong.equals(compositeKeyTimeLongCopy));
		assertFalse(compositeKeyTimeLong.equals(compositeKeyTimeTime));
		assertFalse(compositeKeyTimeLong.equals(compositeKeyLongLong));
		assertFalse(compositeKeyTimeLong.equals(compositeKeyTimeNull));
		assertFalse(compositeKeyTimeLong.equals(compositeKeyNullLong));
		assertFalse(compositeKeyTimeLong.equals(timeFrameKey));
		assertFalse(compositeKeyTimeLong.equals(longKey));
		assertFalse(compositeKeyTimeLong.equals("1"));
		assertFalse(compositeKeyTimeLong.equals(null));

		assertTrue(compositeKeyTimeLongCopy.equals(compositeKeyTimeLong));
		assertFalse(compositeKeyTimeTime.equals(compositeKeyTimeLong));
		assertFalse(compositeKeyLongLong.equals(compositeKeyTimeLong));
		assertFalse(compositeKeyTimeNull.equals(compositeKeyTimeLong));
		assertFalse(compositeKeyNullLong.equals(compositeKeyTimeLong));
		assertFalse(timeFrameKey.equals(compositeKeyTimeLong));
		assertFalse(longKey.equals(compositeKeyTimeLong));
		assertFalse("1".equals(compositeKeyTimeLong));
	}

	@Test
	public void test_toString() {
		TimeFrameKey timeFrameKey = new TimeFrameKey(1, 2, 3);
		LongKey longKey = new LongKey(4L);
		IndexCompositeKey<TimeFrameKey> compositeKeyTime = new IndexCompositeKey<TimeFrameKey>(timeFrameKey, longKey);
		IndexCompositeKey<LongKey> compositeKeyLong = new IndexCompositeKey<LongKey>(longKey, timeFrameKey);

		assertTrue(compositeKeyTime.toString().contains(compositeKeyTime.getClass().getSimpleName()));
		assertTrue(compositeKeyTime.toString().contains(timeFrameKey.toString()));
		assertTrue(compositeKeyTime.toString().contains(longKey.toString()));
		
		assertTrue(compositeKeyLong.toString().contains(compositeKeyTime.getClass().getSimpleName()));
		assertTrue(compositeKeyLong.toString().contains(timeFrameKey.toString()));
		assertTrue(compositeKeyLong.toString().contains(longKey.toString()));
	}

	@SuppressWarnings({"unchecked", "serial", "rawtypes"})
	@Test
	public void test_compareTo() {
		TimeFrameKey timeFrameKey = new TimeFrameKey(1, 2, 3);
		TimeFrameKey timeFrameKeyCopy = new TimeFrameKey(1, 2, 3);
		TimeFrameKey timeFrameKey2 = new TimeFrameKey(2, 4611686018427387906l, 4611686018427387907l);
		TimeFrameKey timeFrameKey3 = new TimeFrameKey(3, 2, 3);
		LongKey longKey = new LongKey(4L);
		LongKey longKeyCopy = new LongKey(4L);
		LongKey longKey5 = new LongKey(5L);
		IndexCompositeKey<TimeFrameKey> compositeKeyTimeLong = new IndexCompositeKey<TimeFrameKey>(timeFrameKey, longKey);
		IndexCompositeKey<TimeFrameKey> compositeKeyTime2Long = new IndexCompositeKey<TimeFrameKey>(timeFrameKey2, longKey);
		IndexCompositeKey<TimeFrameKey> compositeKeyTime3Long = new IndexCompositeKey<TimeFrameKey>(timeFrameKey3, longKey);
		IndexCompositeKey<TimeFrameKey> compositeKeyTimeLong5 = new IndexCompositeKey<TimeFrameKey>(timeFrameKey, longKey5);
		IndexCompositeKey<TimeFrameKey> compositeKeyTime2Long5 = new IndexCompositeKey<TimeFrameKey>(timeFrameKey2, longKey5);
		IndexCompositeKey<LongKey> compositeKeyLongLong = new IndexCompositeKey<LongKey>(longKey, longKey);
		IndexCompositeKey<TimeFrameKey> compositeKeyTimeLongCopy = new IndexCompositeKey<TimeFrameKey>(timeFrameKeyCopy, longKeyCopy);

//		IndexCompositeKey<LongKey> compositeKeyLong = new IndexCompositeKey<LongKey>(longKey, timeFrameKey);

		assertTrue(compositeKeyTimeLong.compareTo(compositeKeyTimeLongCopy) == 0);
		assertTrue(compositeKeyTimeLong.compareTo(null) == -1);
		assertTrue(compositeKeyTimeLong.compareTo(new IndexCompositeKey(longKey, longKey) {}) != 0);
		assertTrue(compositeKeyTimeLong.compareTo(compositeKeyLongLong) != 0);
		assertTrue(compositeKeyTimeLong.compareTo(compositeKeyTimeLong5) < 0);
		assertTrue(compositeKeyTimeLong.compareTo(compositeKeyTime3Long) < 0);
		assertTrue(compositeKeyTimeLong.compareTo(compositeKeyTime2Long5) < 0);
		assertTrue(compositeKeyTimeLong.compareTo(longKey) < 0);
		assertTrue(compositeKeyTime2Long.compareTo(longKey5) > 0); //Grouping numbers are the same but types are different so it ends up ordering by canonical class name
	}

	@Test
	public void test_isInRange() {
		TimeFrameKey _2_to_4 = new TimeFrameKey(1, 2, 4);
		TimeKey _4 = new TimeKey(4, 4);
		TimeKey _5 = new TimeKey(5, 5);
		IndexCompositeKey<TimeKey> compositeKey_2_4 = new IndexCompositeKey<TimeKey>(_2_to_4, _5);
		IndexCompositeKey<TimeKey> compositeKey_4 = new IndexCompositeKey<TimeKey>(_4, _5);

		assertFalse(compositeKey_2_4.isInRange(0, 1));
		assertTrue(compositeKey_2_4.isInRange(0, 2));
		assertTrue(compositeKey_2_4.isInRange(0, 3));
		assertTrue(compositeKey_2_4.isInRange(3, 3));
		assertTrue(compositeKey_2_4.isInRange(0, 6));
		assertTrue(compositeKey_2_4.isInRange(3, 6));
		assertTrue(compositeKey_2_4.isInRange(4, 6));
		assertFalse(compositeKey_2_4.isInRange(5, 6));

		assertFalse(compositeKey_4.isInRange(0, 3));
		assertTrue(compositeKey_4.isInRange(0, 4));
		assertTrue(compositeKey_4.isInRange(0, 6));
		assertTrue(compositeKey_4.isInRange(4, 6));
		assertFalse(compositeKey_4.isInRange(5, 6));
	}
}
