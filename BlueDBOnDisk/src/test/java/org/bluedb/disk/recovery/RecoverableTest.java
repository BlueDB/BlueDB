package org.bluedb.disk.recovery;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class RecoverableTest {

	@Test
	public void test_compareTo() {
		Recoverable<?> time1id1 = new TestRecoverable(1);
		Recoverable<?> time1id1copy = new TestRecoverable(1);
		Recoverable<?> time1id2 = new TestRecoverable(1);
		Recoverable<?> time2id1 = new TestRecoverable(2);
		time1id1.setRecoverableId(1);
		time1id1copy.setRecoverableId(1);
		time1id2.setRecoverableId(2);
		time1id2.setRecoverableId(2);
		
		assertTrue(time1id1.compareTo(time1id1) == 0);
		assertTrue(time1id1.compareTo(time1id1copy) == 0);
		assertTrue(time1id1.compareTo(time1id2) < 0);
		assertTrue(time1id2.compareTo(time1id1) > 0);
		assertTrue(time1id2.compareTo(time2id1) < 0);

		List<Recoverable<?>> recoverables = Arrays.asList(time2id1, time1id1, time1id2);
		List<Recoverable<?>> sorted = Arrays.asList(time1id1, time1id2, time2id1);
		Collections.sort(recoverables);
		assertEquals(sorted, recoverables);
	}

}
