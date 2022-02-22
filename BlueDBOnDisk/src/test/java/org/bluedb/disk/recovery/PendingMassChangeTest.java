package org.bluedb.disk.recovery;

import static org.junit.Assert.*;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.file.FileUtils;
import org.junit.Test;

public class PendingMassChangeTest {

	@Test
	public void testConstructorGettersSettersAndToString() {
		Path path = Paths.get("PendingMassChangeTest");
		PendingMassChange<TestValue> pendingMassChange = new PendingMassChange<TestValue>(5, 10, path);
		assertEquals(5, pendingMassChange.getTimeCreated());
		assertEquals(10, pendingMassChange.getRecoverableId());
		assertEquals("<PendingMassChange for PendingMassChangeTest>", pendingMassChange.toString());
		
		pendingMassChange.setRecoverableId(12);
		assertEquals(12, pendingMassChange.getRecoverableId());
	}
	
	@Test
	public void applyingMassChangeWithEmptyFileDoesEarlyReturn() throws BlueDbException {
		Path path = Paths.get("PendingMassChangeTest");
		PendingMassChange<TestValue> pendingMassChange = new PendingMassChange<TestValue>(5, 10, path);
		pendingMassChange.setRecoverableId(12);
		
		assertTrue(FileUtils.isEmpty(path));
		pendingMassChange.apply(null); //Shouldn't throw an exception or anything, just doesn't really do anything
	}

}
