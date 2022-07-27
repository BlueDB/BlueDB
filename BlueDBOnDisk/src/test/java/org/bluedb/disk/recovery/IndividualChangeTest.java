package org.bluedb.disk.recovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.bluedb.TestUtils;
import org.bluedb.api.Mapper;
import org.bluedb.api.TimeEntityMapper;
import org.bluedb.api.TimeEntityUpdater;
import org.bluedb.api.Updater;
import org.bluedb.api.datastructures.BlueKeyValuePair;
import org.bluedb.api.datastructures.TimeKeyValuePair;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.ActiveTimeKey;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.config.TestDefaultConfigurationService;
import org.bluedb.disk.serialization.BlueEntity;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;
import org.bluedb.disk.serialization.validation.SerializationException;
import org.junit.Test;

public class IndividualChangeTest {
	
	@Test
	public void test_createInsertChange() {
		BlueKey key = new LongKey(42);
		String newValue = "newValue";
		BlueEntity<String> newEntity = new BlueEntity<>(key, newValue);
		BlueKeyValuePair<String> keyValuePair = new BlueKeyValuePair<>(key, newValue);
		
		IndividualChange<String> insertChange = IndividualChange.createInsertChange(keyValuePair);
		
		assertEquals(null, insertChange.getOldValue());
		assertEquals(newValue, insertChange.getNewValue());
		assertEquals(key, insertChange.getKey());
		assertEquals(newEntity, insertChange.getNewEntity());
		assertFalse(insertChange.isKeyChanged());
		assertEquals(key, insertChange.getOriginalKey());
	}

	@Test
	public void test_createDeleteChange() {
		BlueKey key = new LongKey(42);
		String oldValue = "oldValue";
		BlueEntity<String> oldEntity = new BlueEntity<>(key, oldValue);
		
		IndividualChange<String> deleteChange = IndividualChange.createDeleteChange(oldEntity);
		
		assertEquals(oldValue, deleteChange.getOldValue());
		assertEquals(null, deleteChange.getNewValue());
		assertEquals(key, deleteChange.getKey());
		assertEquals(null, deleteChange.getNewEntity());
		assertFalse(deleteChange.isKeyChanged());
		assertEquals(key, deleteChange.getOriginalKey());
	}
	
	@Test
	public void test_createUpdateChange_basicValueMutation() throws SerializationException {
		BlueKey key = new LongKey(42);
		TestValue oldValue = new TestValue("oldValue", 1);
		BlueEntity<TestValue> oldEntity = new BlueEntity<>(key, oldValue);
		Updater<TestValue> updater = value -> value.addCupcake();
		TestValue newValue = new TestValue("oldValue", 2);
		BlueEntity<TestValue> newEntity = new BlueEntity<>(key, newValue);
		
		BlueSerializer serializer = new ThreadLocalFstSerializer(new TestDefaultConfigurationService(), IndividualChange.class);
		
		IndividualChange<TestValue> updateChange = IndividualChange.createUpdateChange(oldEntity, updater, serializer);
		
		assertEquals(oldValue, updateChange.getOldValue());
		assertEquals(newValue, updateChange.getNewValue());
		assertEquals(key, updateChange.getKey());
		assertEquals(newEntity, updateChange.getNewEntity());
		assertFalse(updateChange.isKeyChanged());
		assertEquals(key, updateChange.getOriginalKey());
	}
	
	@Test
	public void test_createUpdateChange_withOldEntityAndNewValue_sameKeys() throws BlueDbException {
		BlueKey key = new LongKey(42);
		TestValue oldValue = new TestValue("oldValue", 1);
		BlueEntity<TestValue> oldEntity = new BlueEntity<>(key, oldValue);
		TestValue newValue = new TestValue("oldValue", 2);
		BlueEntity<TestValue> newEntity = new BlueEntity<>(key, newValue);
		
		IndividualChange<TestValue> updateChange = IndividualChange.createUpdateChange(oldEntity, newEntity.getValue());
		
		assertEquals(oldValue, updateChange.getOldValue());
		assertEquals(newValue, updateChange.getNewValue());
		assertEquals(key, updateChange.getKey());
		assertEquals(newEntity, updateChange.getNewEntity());
		assertFalse(updateChange.isKeyChanged());
		assertEquals(key, updateChange.getOriginalKey());
	}
	
	@Test
	public void test_createUpdateChange_withOldEntityAndNewValue_keyChangeHasNoAffect() throws BlueDbException {
		BlueKey oldKey = new TimeFrameKey(1, 5, 10);
		TestValue oldValue = new TestValue("oldValue", 1);
		BlueEntity<TestValue> oldEntity = new BlueEntity<>(oldKey, oldValue);
		
		BlueKey newKey = new TimeFrameKey(1, 5, 15);
		TestValue newValue = new TestValue("oldValue", 2);
		BlueEntity<TestValue> newEntity = new BlueEntity<>(newKey, newValue);
		
		IndividualChange<TestValue> updateChange = IndividualChange.createUpdateChange(oldEntity, newEntity.getValue());
		
		assertEquals(oldValue, updateChange.getOldValue());
		assertEquals(newValue, updateChange.getNewValue());
		assertEquals(newKey, updateChange.getKey());
		assertEquals(newEntity, updateChange.getNewEntity());
		assertFalse(updateChange.isKeyChanged());
		assertEquals(oldKey, updateChange.getOriginalKey());
	}
	
	@Test
	public void test_createUpdateChange_withOldAndNewEntities_keyUnchanged() throws BlueDbException {
		BlueKey key = new LongKey(42);
		TestValue oldValue = new TestValue("oldValue", 1);
		BlueEntity<TestValue> oldEntity = new BlueEntity<>(key, oldValue);
		TestValue newValue = new TestValue("oldValue", 2);
		BlueEntity<TestValue> newEntity = new BlueEntity<>(key, newValue);
		
		IndividualChange<TestValue> updateChange = IndividualChange.createUpdateKeyAndValueChange(oldEntity, newEntity);
		
		assertEquals(oldValue, updateChange.getOldValue());
		assertEquals(newValue, updateChange.getNewValue());
		assertEquals(key, updateChange.getKey());
		assertEquals(newEntity, updateChange.getNewEntity());
		assertFalse(updateChange.isKeyChanged());
		assertEquals(key, updateChange.getOriginalKey());
	}
	
	@Test
	public void test_createUpdateChange_withOldAndNewEntities_keyChanged() throws BlueDbException {
		BlueKey oldKey = new TimeFrameKey(1, 5, 10);
		TestValue oldValue = new TestValue("oldValue", 1);
		BlueEntity<TestValue> oldEntity = new BlueEntity<>(oldKey, oldValue);
		
		BlueKey newKey = new TimeFrameKey(1, 5, 15);
		TestValue newValue = new TestValue("oldValue", 2);
		BlueEntity<TestValue> newEntity = new BlueEntity<>(newKey, newValue);
		
		IndividualChange<TestValue> updateChange = IndividualChange.createUpdateKeyAndValueChange(oldEntity, newEntity);
		
		assertEquals(oldValue, updateChange.getOldValue());
		assertEquals(newValue, updateChange.getNewValue());
		assertEquals(newKey, updateChange.getKey());
		assertEquals(newEntity, updateChange.getNewEntity());
		assertTrue(updateChange.isKeyChanged());
		assertEquals(oldKey, updateChange.getOriginalKey());
	}
	
	@Test
	public void test_createUpdateChange_withOldAndNewEntities_invalidKeyChange() throws BlueDbException {
		BlueKey oldKey = new TimeFrameKey(1, 5, 10);
		TestValue oldValue = new TestValue("oldValue", 1);
		BlueEntity<TestValue> oldEntity = new BlueEntity<>(oldKey, oldValue);
		
		BlueKey newKey = new TimeFrameKey(1, 3, 10);
		TestValue newValue = new TestValue("oldValue", 2);
		BlueEntity<TestValue> newEntity = new BlueEntity<>(newKey, newValue);
		
		try {
			IndividualChange.createUpdateKeyAndValueChange(oldEntity, newEntity);
			fail("You should be able to change the start time on a time frame key. Only the end time.");
		} catch(BlueDbException e) { /* Expected */ }
	}
	
	@Test
	public void test_createUpdateKeyAndValueChange_keyChange() throws BlueDbException {
		BlueKey key = new ActiveTimeKey(1, 1);
		TestValue oldValue = new TestValue("oldValue", 1);
		BlueEntity<TestValue> oldEntity = new BlueEntity<>(key, oldValue);
		TimeEntityUpdater<TestValue> timeEntityUpdater = value -> {
				value.addCupcake();
				return new TimeFrameKey(1, 1, 10);
		};
		BlueKey newKey = new TimeFrameKey(1, 1, 10);
		TestValue newValue = new TestValue("oldValue", 2);
		BlueEntity<TestValue> newEntity = new BlueEntity<>(newKey, newValue);
		
		BlueSerializer serializer = new ThreadLocalFstSerializer(new TestDefaultConfigurationService(), IndividualChange.class);
		
		IndividualChange<TestValue> updateChange = IndividualChange.createUpdateKeyAndValueChange(oldEntity.getKey(), oldEntity.getValue(), timeEntityUpdater, serializer);
		
		assertEquals(oldValue, updateChange.getOldValue());
		assertEquals(newValue, updateChange.getNewValue());
		assertEquals(newKey, updateChange.getKey());
		assertEquals(newEntity, updateChange.getNewEntity());
		assertTrue(updateChange.isKeyChanged());
		assertEquals(key, updateChange.getOriginalKey());
	}
	
	@Test
	public void test_createUpdateKeyAndValueChange_keyUnchanged() throws BlueDbException {
		BlueKey key = new ActiveTimeKey(1, 1);
		TestValue oldValue = new TestValue("oldValue", 1);
		BlueEntity<TestValue> oldEntity = new BlueEntity<>(key, oldValue);
		TimeEntityUpdater<TestValue> timeEntityUpdater = value -> {
				value.addCupcake();
				return new ActiveTimeKey(1, 1);
		};
		BlueKey newKey = new TimeFrameKey(1, 1, 10);
		TestValue newValue = new TestValue("oldValue", 2);
		BlueEntity<TestValue> newEntity = new BlueEntity<>(newKey, newValue);
		
		BlueSerializer serializer = new ThreadLocalFstSerializer(new TestDefaultConfigurationService(), IndividualChange.class);
		
		IndividualChange<TestValue> updateChange = IndividualChange.createUpdateKeyAndValueChange(oldEntity.getKey(), oldEntity.getValue(), timeEntityUpdater, serializer);
		
		assertEquals(oldValue, updateChange.getOldValue());
		assertEquals(newValue, updateChange.getNewValue());
		assertEquals(key, updateChange.getKey());
		assertEquals(newEntity, updateChange.getNewEntity());
		assertFalse(updateChange.isKeyChanged());
		assertEquals(key, updateChange.getOriginalKey());
	}
	
	@Test
	public void test_createUpdateKeyAndValueChange_invalidKeyChange() throws BlueDbException {
		BlueKey key = new ActiveTimeKey(1, 1);
		TestValue oldValue = new TestValue("oldValue", 1);
		BlueEntity<TestValue> oldEntity = new BlueEntity<>(key, oldValue);
		TimeEntityUpdater<TestValue> timeEntityUpdater = value -> {
				value.addCupcake();
				return new ActiveTimeKey(2, 1);
		};
		
		BlueSerializer serializer = new ThreadLocalFstSerializer(new TestDefaultConfigurationService(), IndividualChange.class);
		
		try {
			IndividualChange.createUpdateKeyAndValueChange(oldEntity.getKey(), oldEntity.getValue(), timeEntityUpdater, serializer);
			fail("You shouldn't be able to change the id on the key.");
		} catch(BlueDbException e) { /* Expected */ }
	}
	
	@Test
	public void test_manuallyCreateTestChange() {
		BlueKey key = new LongKey(42);
		String oldValue = "oldValue";
		String newValue = "newValue";
		BlueEntity<String> newEntity = new BlueEntity<>(key, newValue);
		
		IndividualChange<String> updateChange = IndividualChange.manuallyCreateTestChange(key, oldValue, newValue, Optional.empty());
		
		assertEquals(oldValue, updateChange.getOldValue());
		assertEquals(newValue, updateChange.getNewValue());
		assertEquals(key, updateChange.getKey());
		assertEquals(newEntity, updateChange.getNewEntity());
		assertFalse(updateChange.isKeyChanged());
		assertEquals(key, updateChange.getOriginalKey());
	}
	
	@Test
	public void test_createReplaceChange() throws SerializationException {
		BlueKey key = new LongKey(42);
		
		TestValue oldValue = new TestValue("oldValue", 1);
		BlueEntity<TestValue> oldEntity = new BlueEntity<>(key, oldValue);
		
		TestValue newValue = new TestValue("oldValue", 2);
		BlueEntity<TestValue> newEntity = new BlueEntity<>(key, newValue);

		Mapper<TestValue> mapper = value -> {
			return newValue;
		};
		
		BlueSerializer serializer = new ThreadLocalFstSerializer(new TestDefaultConfigurationService(), IndividualChange.class);
		
		IndividualChange<TestValue> replaceChange = IndividualChange.createReplaceChange(oldEntity, mapper, serializer);
		
		assertEquals(oldValue, replaceChange.getOldValue());
		assertEquals(newValue, replaceChange.getNewValue());
		assertEquals(key, replaceChange.getKey());
		assertEquals(newEntity, replaceChange.getNewEntity());
		assertFalse(replaceChange.isKeyChanged());
		assertEquals(key, replaceChange.getOriginalKey());
	}
	
	@Test
	public void test_createReplaceKeyAndValueChange_keyChanged() throws BlueDbException {
		TimeFrameKey oldKey = new TimeFrameKey(42, 0, 100);
		TestValue oldValue = new TestValue("oldValue", 1);
		
		ActiveTimeKey newKey = new ActiveTimeKey(42, 0);
		TestValue newValue = new TestValue("oldValue", 2);
		BlueEntity<TestValue> newEntity = new BlueEntity<>(newKey, newValue);

		TimeEntityMapper<TestValue> mapper = value -> {
			return new TimeKeyValuePair<TestValue>(newKey, newValue);
		};
		
		BlueSerializer serializer = new ThreadLocalFstSerializer(new TestDefaultConfigurationService(), IndividualChange.class);
		
		IndividualChange<TestValue> replaceChange = IndividualChange.createReplaceKeyAndValueChange(oldKey, oldValue, mapper, serializer);
		
		assertEquals(oldValue, replaceChange.getOldValue());
		assertEquals(newValue, replaceChange.getNewValue());
		assertEquals(newKey, replaceChange.getKey());
		assertEquals(newEntity, replaceChange.getNewEntity());
		assertTrue(replaceChange.isKeyChanged());
		assertEquals(oldKey, replaceChange.getOriginalKey());
	}
	
	@Test
	public void test_createReplaceKeyAndValueChange_keyUnchanged() throws BlueDbException {
		TimeFrameKey oldKey = new TimeFrameKey(42, 0, 100);
		TestValue oldValue = new TestValue("oldValue", 1);
		
		TestValue newValue = new TestValue("oldValue", 2);
		BlueEntity<TestValue> newEntity = new BlueEntity<>(oldKey, newValue);

		TimeEntityMapper<TestValue> mapper = value -> {
			return new TimeKeyValuePair<TestValue>(oldKey, newValue);
		};
		
		BlueSerializer serializer = new ThreadLocalFstSerializer(new TestDefaultConfigurationService(), IndividualChange.class);
		
		IndividualChange<TestValue> replaceChange = IndividualChange.createReplaceKeyAndValueChange(oldKey, oldValue, mapper, serializer);
		
		assertEquals(oldValue, replaceChange.getOldValue());
		assertEquals(newValue, replaceChange.getNewValue());
		assertEquals(oldKey, replaceChange.getKey());
		assertEquals(newEntity, replaceChange.getNewEntity());
		assertFalse(replaceChange.isKeyChanged());
		assertEquals(oldKey, replaceChange.getOriginalKey());
	}
	
	@Test
	public void test_createReplaceKeyAndValueChange_invalidKeyChange() throws BlueDbException {
		TimeFrameKey oldKey = new TimeFrameKey(42, 0, 100);
		TestValue oldValue = new TestValue("oldValue", 1);
		
		TimeFrameKey newKey = new TimeFrameKey(43, 0, 100);
		TestValue newValue = new TestValue("oldValue", 2);

		TimeEntityMapper<TestValue> mapper = value -> {
			return new TimeKeyValuePair<TestValue>(newKey, newValue);
		};
		
		BlueSerializer serializer = new ThreadLocalFstSerializer(new TestDefaultConfigurationService(), IndividualChange.class);
		
		try {
			IndividualChange.createReplaceKeyAndValueChange(oldKey, oldValue, mapper, serializer);
			fail("You shouldn't be able to change the id on the key.");
		} catch(BlueDbException e) { /* expected */ }
	}
	
	@Test
	public void testHashEquals() {
		IndividualChange<TestValue> change1 = IndividualChange.manuallyCreateTestChange(new TimeKey(1, 1), new TestValue("Bob"), new TestValue("Bob", 5), Optional.empty());
		IndividualChange<TestValue> change1Clone = IndividualChange.manuallyCreateTestChange(new TimeKey(1, 1), new TestValue("Bob"), new TestValue("Bob", 5), Optional.empty());
		
		IndividualChange<TestValue> changeWithDifferentKey = IndividualChange.manuallyCreateTestChange(new TimeKey(2, 2), new TestValue("Bob"), new TestValue("Bob", 5), Optional.empty());
		IndividualChange<TestValue> changeWithDifferentOldValue = IndividualChange.manuallyCreateTestChange(new TimeKey(1, 1), new TestValue("Bob", 3), new TestValue("Bob", 5), Optional.empty());
		IndividualChange<TestValue> changeWithDifferentNewValue = IndividualChange.manuallyCreateTestChange(new TimeKey(1, 1), new TestValue("Bob"), new TestValue("Bob", 8), Optional.empty());
		
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
		IndividualChange.manuallyCreateTestChange(null, null, null, Optional.empty()).toString();
	}
	
	@Test
	public void test_serialization() throws URISyntaxException, IOException, BlueDbException {
		Path v0UnregisteredChangePath = TestUtils.getResourcePath("individual_changes/v0-change-not-registered.bin");
		Path v0RegisteredChangePath = TestUtils.getResourcePath("individual_changes/v0-change-registered.bin");
		Path v1RegisteredChangePath = TestUtils.getResourcePath("individual_changes/v1-change-registered.bin");
		
		BlueSerializer serializer = new ThreadLocalFstSerializer(new TestDefaultConfigurationService(), IndividualChange.class);
		
		TimeKey oldKey = new TimeKey(1, 1);
		TestValue oldValue = new TestValue("Bob", 1);
		TimeKey newKey = new TimeFrameKey(1, 1, 10);
		TestValue newValue = new TestValue("Bob", 2);
		
		IndividualChange<TestValue> change = IndividualChange.createUpdateKeyAndValueChange(new BlueEntity<>(oldKey, oldValue), new BlueEntity<>(newKey, newValue));
		IndividualChange<TestValue> expectedWhenDeserializingOldChanges = IndividualChange.manuallyCreateTestChange(newKey, oldValue, newValue, Optional.empty());
		
//		Files.write(v1RegisteredChangePath, serializer.serializeObjectToByteArray(change));
		
		assertEquals(expectedWhenDeserializingOldChanges, serializer.deserializeObjectFromByteArray(Files.readAllBytes(v0UnregisteredChangePath)));
		assertEquals(expectedWhenDeserializingOldChanges, serializer.deserializeObjectFromByteArray(Files.readAllBytes(v0RegisteredChangePath)));
		assertEquals(change, serializer.deserializeObjectFromByteArray(Files.readAllBytes(v1RegisteredChangePath)));
	}

}
