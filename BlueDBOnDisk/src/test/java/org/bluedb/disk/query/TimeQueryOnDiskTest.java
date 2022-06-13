package org.bluedb.disk.query;

import static org.junit.Assert.*;

import org.bluedb.disk.TestValue;
import org.bluedb.disk.query.ReadOnlyQueryOnDisk.TimeIncludeMode;
import org.junit.Test;

public class TimeQueryOnDiskTest {

	@Test
	public void testWhereKeyIsActive() {
		TimeQueryOnDisk<TestValue> query = new TimeQueryOnDisk<TestValue>(null);
		assertEquals(TimeIncludeMode.INCLUDE_ALL, query.timeIncludeMode);
		query.whereKeyIsActive();
		assertEquals(TimeIncludeMode.INCLUDE_ONLY_ACTIVE, query.timeIncludeMode);
		try {
			query.whereKeyIsNotActive();
			fail("You can't call whereKeyIsNotActive after calling whereKeyIsActive");
		} catch(IllegalStateException e) { /* expected */ }
	}

	@Test
	public void testWhereKeyIsNotActive() {
		TimeQueryOnDisk<TestValue> query = new TimeQueryOnDisk<TestValue>(null);
		assertEquals(TimeIncludeMode.INCLUDE_ALL, query.timeIncludeMode);
		query.whereKeyIsNotActive();
		assertEquals(TimeIncludeMode.EXCLUDE_ACTIVE, query.timeIncludeMode);
		try {
			query.whereKeyIsActive();
			fail("You can't call whereKeyIsActive after calling whereKeyIsNotActive");
		} catch(IllegalStateException e) { /* expected */ }
	}

}
