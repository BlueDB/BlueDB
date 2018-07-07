package io.bluedb.disk;

import static org.junit.Assert.*;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import io.bluedb.api.Condition;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.file.BlueObjectInput;
import io.bluedb.disk.file.BlueObjectOutput;
import io.bluedb.disk.file.BlueReadLock;
import io.bluedb.disk.file.BlueWriteLock;
import io.bluedb.disk.file.LockManager;

public class BlutilsTest {

	@Test
	public void test_meetsConditions() {
		List<Condition<Long>> empty = Arrays.asList();
		List<Condition<Long>> greaterThan1 = Arrays.asList((l) -> l > 1);
		List<Condition<Long>> between1and3 = Arrays.asList((l) -> l > 1, (l) -> l < 3);
		assertTrue(Blutils.meetsConditions(empty, 1L));
		assertFalse(Blutils.meetsConditions(greaterThan1, 1L));
		assertTrue(Blutils.meetsConditions(greaterThan1, 2L));
		assertFalse(Blutils.meetsConditions(between1and3, 1L));
		assertFalse(Blutils.meetsConditions(between1and3, 3L));
		assertTrue(Blutils.meetsConditions(between1and3, 2L));
	}

	@Test
	public void test_roundDownToMultiple() {
		assertEquals(0, Blutils.roundDownToMultiple(0, 2));  // test zero
		assertEquals(4, Blutils.roundDownToMultiple(5, 2));  // test greater than a multiple
		assertEquals(0, Blutils.roundDownToMultiple(41, 42));  // test equal to a multiple
		assertEquals(42, Blutils.roundDownToMultiple(42, 42));  // test equal to a multiple
		assertTrue(Blutils.roundDownToMultiple(Long.MAX_VALUE, 100) > 0); // make sure we don't overflow
		assertTrue(Blutils.roundDownToMultiple(Long.MIN_VALUE, 100) < 0); // make sure we don't overflow
		assertEquals(-100L, Blutils.roundDownToMultiple(-10, 100));
		assertEquals(-100L, Blutils.roundDownToMultiple(-1, 100));
		assertEquals(-100L, Blutils.roundDownToMultiple(-100, 100));
	}

	@Test
	public void test_filter() {
		List<Long> values = Arrays.asList(7L, 5L, 1000L, 1L);
		List<Long> valuesBiggerThan2 = Arrays.asList(7L, 5L, 1000L);
		List<Long> filteredValues = Blutils.filter(values, (l) -> (l > 2));
		assertEquals(valuesBiggerThan2, filteredValues);
	}

	@Test
	public void test_inRange() {
		BlueKey _2_to_4 = new TimeFrameKey(1, 2, 4);
		BlueKey _4 = new TimeKey(4, 4);
		assertFalse(Blutils.isInRange(_2_to_4, 0, 1));
		assertTrue(Blutils.isInRange(_2_to_4, 0, 2));
		assertTrue(Blutils.isInRange(_2_to_4, 0, 3));
		assertTrue(Blutils.isInRange(_2_to_4, 3, 3));
		assertTrue(Blutils.isInRange(_2_to_4, 0, 6));
		assertTrue(Blutils.isInRange(_2_to_4, 3, 6));
		assertTrue(Blutils.isInRange(_2_to_4, 4, 6));
		assertFalse(Blutils.isInRange(_2_to_4, 5, 6));
		
		assertFalse(Blutils.isInRange(_4, 0, 3));
		assertTrue(Blutils.isInRange(_4, 0, 4));
		assertTrue(Blutils.isInRange(_4, 0, 6));
		assertTrue(Blutils.isInRange(_4, 4, 6));
		assertFalse(Blutils.isInRange(_4, 5, 6));
}
}
