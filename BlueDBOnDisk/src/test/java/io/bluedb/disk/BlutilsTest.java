package io.bluedb.disk;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import io.bluedb.api.Condition;

public class BlutilsTest {

	@Test
	public void test_constructor() {
		new Blutils(); // this doesn't really test anything, it just makes code coverage 100%
	}

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
	public void test_map() throws Exception {
		List<String> originalValues = Arrays.asList("7", "5", "1000");
		List<Long> expectedResults = Arrays.asList(7L, 5L, 1000L);
		List<Long> mappedValues = Blutils.map(originalValues, (s) -> Long.valueOf(s));
		assertEquals(expectedResults, mappedValues);
	}

	@Test
	public void test_sortByMappedValue() {
		List<String> listToSort = Arrays.asList("7", "5", "1000");
		List<String> valuesInOrder = Arrays.asList("5", "7", "1000");
		Blutils.sortByMappedValue(listToSort, (s) -> Long.valueOf(s) );
		assertEquals(valuesInOrder, listToSort);
	}

	@Test
	public void test_trySleep() {
		AtomicBoolean isDoneSleeping = new AtomicBoolean(false);
		Thread sleepingThread = new Thread(new Runnable() {
			@Override
			public void run() {
				Blutils.trySleep(20_000);
				isDoneSleeping.set(true);
			}
		});
		sleepingThread.start();
		assertFalse(isDoneSleeping.get());
		Blutils.trySleep(10);
		assertFalse(isDoneSleeping.get());
		sleepingThread.interrupt();
		try {
			sleepingThread.join(2_000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		assertTrue(isDoneSleeping.get());
	}
}
