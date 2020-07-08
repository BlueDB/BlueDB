package org.bluedb.disk.recovery;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.TestValue;
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
	
	@Test
	public void testHashEquals() {
		IndividualChange<TestValue> change1 = new IndividualChange<>(new TimeKey(1, 1), new TestValue("Bob"), new TestValue("Bob", 5));
		IndividualChange<TestValue> change1Clone = new IndividualChange<>(new TimeKey(1, 1), new TestValue("Bob"), new TestValue("Bob", 5));
		
		IndividualChange<TestValue> changeWithDifferentKey = new IndividualChange<>(new TimeKey(2, 2), new TestValue("Bob"), new TestValue("Bob", 5));
		IndividualChange<TestValue> changeWithDifferentOldValue = new IndividualChange<>(new TimeKey(1, 1), new TestValue("Bob", 3), new TestValue("Bob", 5));
		IndividualChange<TestValue> changeWithDifferentNewValue = new IndividualChange<>(new TimeKey(1, 1), new TestValue("Bob"), new TestValue("Bob", 8));
		
		assertEquals(change1, change1);
		assertEquals(change1, change1Clone);
		assertNotEquals(change1, changeWithDifferentKey);
		assertNotEquals(change1, changeWithDifferentOldValue);
		assertNotEquals(change1, changeWithDifferentNewValue);
		assertNotEquals(change1, null);
		
		Set<IndividualChange<TestValue>> set = new HashSet<>();
		set.add(change1);
		assertTrue(set.contains(change1Clone));
	}
	
	@Test
	public void test_toString() {
		/*
		 * We don't want toString to show up as uncovered code. This also verifies that 
		 * null values don't result in an NPE
		 */
		new IndividualChange<>(null, null, null).toString();
	}

}
