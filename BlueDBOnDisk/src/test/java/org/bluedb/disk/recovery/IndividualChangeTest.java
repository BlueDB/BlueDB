package org.bluedb.disk.recovery;

import static org.junit.Assert.*;

import org.junit.Test;

import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.disk.serialization.BlueEntity;

public class IndividualChangeTest {

	@Test
	public void test() {
		BlueKey key = new LongKey(42);
		String oldValue = "oldValue";
		String newValue = "newValue";
		BlueEntity<String> newEntity = new BlueEntity<>(key, newValue);
		IndividualChange<String> addChange = new IndividualChange<>(key, null, newValue);
		IndividualChange<String> deleteChange = new IndividualChange<>(key, oldValue, null);
		IndividualChange<String> updateChange = new IndividualChange<>(key, oldValue, newValue);
		assertEquals(null, addChange.getOldValue());
		assertEquals(newValue, addChange.getNewValue());
		assertEquals(oldValue, deleteChange.getOldValue());
		assertEquals(null, deleteChange.getNewValue());
		assertEquals(oldValue, updateChange.getOldValue());
		assertEquals(newValue, updateChange.getNewValue());
		assertEquals(key, addChange.getKey());
		assertEquals(key, deleteChange.getKey());
		assertEquals(key, updateChange.getKey());
		assertEquals(newEntity, addChange.getNewEntity());
		assertEquals(null, deleteChange.getNewEntity());
		assertEquals(newEntity, updateChange.getNewEntity());
	}

}
