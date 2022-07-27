package org.bluedb.disk.query;

import static org.junit.Assert.*;

import org.bluedb.disk.TestValue;
import org.bluedb.disk.query.ReadOnlyQueryOnDisk.TimeIncludeMode;
import org.junit.Test;

public class ReadOnlyQueryOnDiskTest {

	@Test
	public void testWhereKeyIsActive() {
		ReadOnlyQueryOnDisk<TestValue> query = new ReadOnlyQueryOnDisk<TestValue>(null);
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
		ReadOnlyQueryOnDisk<TestValue> query = new ReadOnlyQueryOnDisk<TestValue>(null);
		assertEquals(TimeIncludeMode.INCLUDE_ALL, query.timeIncludeMode);
		query.whereKeyIsNotActive();
		assertEquals(TimeIncludeMode.EXCLUDE_ACTIVE, query.timeIncludeMode);
		try {
			query.whereKeyIsActive();
			fail("You can't call whereKeyIsActive after calling whereKeyIsNotActive");
		} catch(IllegalStateException e) { /* expected */ }
	}

}
