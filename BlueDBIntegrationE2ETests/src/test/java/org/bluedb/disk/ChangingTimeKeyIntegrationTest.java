package org.bluedb.disk;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.bluedb.api.BlueCollectionVersion;
import org.bluedb.api.datastructures.BlueKeyValuePair;
import org.bluedb.api.datastructures.TimeKeyValuePair;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.ActiveTimeKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.collection.ReadWriteTimeCollectionOnDisk;
import org.bluedb.disk.helpers.BlueDbOnDiskWrapper;
import org.bluedb.disk.helpers.BlueDbOnDiskWrapper.StartupOption;
import org.junit.Test;

public class ChangingTimeKeyIntegrationTest {
	private long oneHour = TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
	private long now = System.currentTimeMillis();
	private long oneHourAgo = now - oneHour;
	private long twoHoursAgo = now - oneHour*2;
//	private long threeHoursAgo = now - oneHour*3;
	private long fourHoursAgo = now - oneHour*4;
//	private long fiveHoursAgo = now - oneHour*5;
//	private long sixHoursAgo = now - oneHour*6;
	private long sevenHoursAgo = now - oneHour*7;
	private long eightHoursAgo = now - oneHour*8;
	
	private ReadWriteTimeCollectionOnDisk<TestValue> timeCollection;
	
	private TestValue staticCompletedValue;
	private TestValue staticActiveValue;
	
	private ActiveTimeKey activeKey;
	private TimeFrameKey timeFrameKey1;
	private TimeFrameKey timeFrameKey2;
	private TimeKey timeKey;
	
	private TestValue testValue;
	private List<Object> emptyList;
	private List<TestValue> testValueAndStaticActiveValue;
	private List<TestValue> testValueAndStaticCompletedValue;
	private List<TestValue> staticActiveValueOnly;
	private List<TestValue> staticCompletedValueOnly;
	private List<TestValue> staticValues;
	private List<TestValue> allValues;
	
	@Test
	public void test_changingTimeKeyTypes() throws IOException, BlueDbException {
		try (BlueDbOnDiskWrapper dbWrapper = new BlueDbOnDiskWrapper(StartupOption.EncryptionDisabled, BlueCollectionVersion.VERSION_2)) {
			timeCollection = dbWrapper.getTimeCollection();
			
			staticCompletedValue = new TestValue("Jane");
			timeCollection.insert(new TimeFrameKey(UUID.randomUUID(), sevenHoursAgo, oneHourAgo), staticCompletedValue);
			
			staticActiveValue = new TestValue("Charlie");
			timeCollection.insert(new ActiveTimeKey(UUID.randomUUID(), sevenHoursAgo - 10), staticActiveValue);
			
			UUID id = UUID.randomUUID();
			activeKey = new ActiveTimeKey(id, sevenHoursAgo - 20);
			timeFrameKey1 = new TimeFrameKey(id, sevenHoursAgo - 20, fourHoursAgo);
			timeFrameKey2 = new TimeFrameKey(id, sevenHoursAgo - 20, twoHoursAgo);
			timeKey = new TimeKey(id, sevenHoursAgo - 20);
			testValue = new TestValue("Bob", 365);
			
			emptyList = Arrays.asList();
			testValueAndStaticActiveValue = Arrays.asList(testValue, staticActiveValue);
			testValueAndStaticCompletedValue = Arrays.asList(testValue, staticCompletedValue);
			staticActiveValueOnly = Arrays.asList(staticActiveValue);
			staticCompletedValueOnly = Arrays.asList(staticCompletedValue);
			staticValues = Arrays.asList(staticActiveValue, staticCompletedValue);
			allValues = Arrays.asList(testValue, staticActiveValue, staticCompletedValue);
			
			testWithCollectionInsertAndUpdate();
			testWithCollectionInsertAndReplace();
			testWithBatchUpsert();
			testWithQueryUpdate();
			testWithQueryReplace();
		}
	}

	private void testWithCollectionInsertAndUpdate() throws BlueDbException {
		TestTimeKeyUpdater taskToInsertActiveKey = () -> timeCollection.insert(activeKey, testValue);
		TestTimeKeyUpdater taskToChangeToTimeFrameKey1 = () -> {
			testValue.addCupcake();
			timeCollection.update(timeFrameKey1, valueToUpdate -> valueToUpdate.addCupcake());
		};
		TestTimeKeyUpdater taskToChangeToTimeFrameKey2 = () -> {
			testValue.addCupcake();
			timeCollection.update(timeFrameKey2, valueToUpdate -> valueToUpdate.addCupcake());	
		};
		TestTimeKeyUpdater taskToChangeToActiveTimeKey = () -> {
			testValue.addCupcake();
			timeCollection.update(activeKey, valueToUpdate -> valueToUpdate.addCupcake());
		};
		TestTimeKeyUpdater taskToChangeToTimeKey = () -> {
			testValue.addCupcake();
			timeCollection.update(timeKey, valueToUpdate -> valueToUpdate.addCupcake());
		};
		testChangingKeyTypesStartingWithActiveTestValue(taskToInsertActiveKey, taskToChangeToTimeFrameKey1, taskToChangeToTimeFrameKey2, taskToChangeToActiveTimeKey, taskToChangeToTimeKey);
	}

	private void testWithCollectionInsertAndReplace() throws BlueDbException {
		TestTimeKeyUpdater taskToInsertActiveKey = () -> timeCollection.insert(activeKey, testValue);
		TestTimeKeyUpdater taskToChangeToTimeFrameKey1 = () -> {
			testValue.addCupcake();
			timeCollection.replace(timeFrameKey1, valueToUpdate -> testValue);
		};
		TestTimeKeyUpdater taskToChangeToTimeFrameKey2 = () -> {
			testValue.addCupcake();
			timeCollection.replace(timeFrameKey2, valueToUpdate -> testValue);	
		};
		TestTimeKeyUpdater taskToChangeToActiveTimeKey = () -> {
			testValue.addCupcake();
			timeCollection.replace(activeKey, valueToUpdate -> testValue);
		};
		TestTimeKeyUpdater taskToChangeToTimeKey = () -> {
			testValue.addCupcake();
			timeCollection.replace(timeKey, valueToUpdate -> testValue);
		};
		testChangingKeyTypesStartingWithActiveTestValue(taskToInsertActiveKey, taskToChangeToTimeFrameKey1, taskToChangeToTimeFrameKey2, taskToChangeToActiveTimeKey, taskToChangeToTimeKey);
	}

	private void testWithBatchUpsert() throws BlueDbException {
		TestTimeKeyUpdater taskToInsertActiveKey = () -> {
			timeCollection.batchUpsert(Arrays.asList(new BlueKeyValuePair<>(activeKey, testValue)).iterator());
		};
		TestTimeKeyUpdater taskToChangeToTimeFrameKey1 = () -> {
			testValue.addCupcake();
			timeCollection.batchUpsert(Arrays.asList(new BlueKeyValuePair<>(timeFrameKey1, testValue)).iterator());
		};
		TestTimeKeyUpdater taskToChangeToTimeFrameKey2 = () -> {
			testValue.addCupcake();
			timeCollection.batchUpsert(Arrays.asList(new BlueKeyValuePair<>(timeFrameKey2, testValue)).iterator());
		};
		TestTimeKeyUpdater taskToChangeToActiveTimeKey = () -> {
			testValue.addCupcake();
			timeCollection.batchUpsert(Arrays.asList(new BlueKeyValuePair<>(activeKey, testValue)).iterator());
		};
		TestTimeKeyUpdater taskToChangeToTimeKey = () -> {
			testValue.addCupcake();
			timeCollection.batchUpsert(Arrays.asList(new BlueKeyValuePair<>(timeKey, testValue)).iterator());
		};
		testChangingKeyTypesStartingWithActiveTestValue(taskToInsertActiveKey, taskToChangeToTimeFrameKey1, taskToChangeToTimeFrameKey2, taskToChangeToActiveTimeKey, taskToChangeToTimeKey);
	}

	private void testWithQueryUpdate() throws BlueDbException {
		TestTimeKeyUpdater taskToInsertActiveKey = () -> {
			timeCollection.batchUpsert(Arrays.asList(new BlueKeyValuePair<>(activeKey, testValue)).iterator());
		};
		TestTimeKeyUpdater taskToChangeToTimeFrameKey1 = () -> {
			testValue.addCupcake();
			timeCollection.query()
				.whereKeyIsIn(new HashSet<>(Arrays.asList(activeKey)))
				.updateKeyAndValue(valueToUpdate -> {
					valueToUpdate.addCupcake();
					return timeFrameKey1;
				});
		};
		TestTimeKeyUpdater taskToChangeToTimeFrameKey2 = () -> {
			testValue.addCupcake();
			timeCollection.query()
				.whereKeyIsIn(new HashSet<>(Arrays.asList(activeKey)))
				.updateKeyAndValue(valueToUpdate -> {
					valueToUpdate.addCupcake();
					return timeFrameKey2;
				});
		};
		TestTimeKeyUpdater taskToChangeToActiveTimeKey = () -> {
			testValue.addCupcake();
			timeCollection.query()
				.whereKeyIsIn(new HashSet<>(Arrays.asList(activeKey)))
				.updateKeyAndValue(valueToUpdate -> {
					valueToUpdate.addCupcake();
					return activeKey;
				});
		};
		TestTimeKeyUpdater taskToChangeToTimeKey = () -> {
			testValue.addCupcake();
			timeCollection.query()
			.whereKeyIsIn(new HashSet<>(Arrays.asList(activeKey)))
			.updateKeyAndValue(valueToUpdate -> {
				valueToUpdate.addCupcake();
				return timeKey;
			});
		};
		testChangingKeyTypesStartingWithActiveTestValue(taskToInsertActiveKey, taskToChangeToTimeFrameKey1, taskToChangeToTimeFrameKey2, taskToChangeToActiveTimeKey, taskToChangeToTimeKey);
	}

	private void testWithQueryReplace() throws BlueDbException {
		TestTimeKeyUpdater taskToInsertActiveKey = () -> {
			timeCollection.batchUpsert(Arrays.asList(new BlueKeyValuePair<>(activeKey, testValue)).iterator());
		};
		TestTimeKeyUpdater taskToChangeToTimeFrameKey1 = () -> {
			testValue.addCupcake();
			timeCollection.query()
				.whereKeyIsIn(new HashSet<>(Arrays.asList(activeKey)))
				.replaceKeyAndValue(valueToUpdate -> {
					return new TimeKeyValuePair<>(timeFrameKey1, testValue);
				});
		};
		TestTimeKeyUpdater taskToChangeToTimeFrameKey2 = () -> {
			testValue.addCupcake();
			timeCollection.query()
				.whereKeyIsIn(new HashSet<>(Arrays.asList(activeKey)))
				.replaceKeyAndValue(valueToUpdate -> {
					return new TimeKeyValuePair<>(timeFrameKey2, testValue);
				});
		};
		TestTimeKeyUpdater taskToChangeToActiveTimeKey = () -> {
			testValue.addCupcake();
			timeCollection.query()
				.whereKeyIsIn(new HashSet<>(Arrays.asList(activeKey)))
				.replaceKeyAndValue(valueToUpdate -> {
					return new TimeKeyValuePair<>(activeKey, testValue);
				});
		};
		TestTimeKeyUpdater taskToChangeToTimeKey = () -> {
			testValue.addCupcake();
			timeCollection.query()
			.whereKeyIsIn(new HashSet<>(Arrays.asList(activeKey)))
			.replaceKeyAndValue(valueToUpdate -> {
				return new TimeKeyValuePair<>(timeKey, testValue);
			});
		};
		testChangingKeyTypesStartingWithActiveTestValue(taskToInsertActiveKey, taskToChangeToTimeFrameKey1, taskToChangeToTimeFrameKey2, taskToChangeToActiveTimeKey, taskToChangeToTimeKey);
	}

	private void testChangingKeyTypesStartingWithActiveTestValue(
			TestTimeKeyUpdater taskToInsertActiveKey,
			TestTimeKeyUpdater taskToChangeToTimeFrameKey1,
			TestTimeKeyUpdater taskToChangeToTimeFrameKey2,
			TestTimeKeyUpdater taskToChangeToActiveTimeKey,
			TestTimeKeyUpdater taskToChangeToTimeKey) throws BlueDbException {

		taskToInsertActiveKey.execute();
		verifyEverythingWorksAfterInsertingActiveTestValue();
		
		taskToChangeToTimeFrameKey1.execute();
		verifyEverythingWorksAfterChangingToTimeFrameKey1();
		
		taskToChangeToTimeFrameKey2.execute();
		verifyEverythingWorksAfterChangingToTimeFrameKey2();
		
		taskToChangeToActiveTimeKey.execute();
		verifyEverythingWorksAfterChangingToActiveTimeKey();
		
		taskToChangeToTimeKey.execute();
		verifyEverythingWorksAfterChangingToTimeKey();
		
		timeCollection.delete(activeKey);
	}

	private void verifyEverythingWorksAfterInsertingActiveTestValue() throws BlueDbException {
		verifyEverythingWorksAfterChangingToActiveTimeKey();
	}

	private void verifyEverythingWorksAfterChangingToTimeFrameKey1() throws BlueDbException {
		assertEquals(testValue, timeCollection.get(activeKey));
		assertEquals(allValues, timeCollection.query()
			.afterTime(eightHoursAgo)
			.beforeTime(now)
			.getList());
		assertEquals(allValues, timeCollection.query()
				.afterOrAtTime(fourHoursAgo - 1)
				.beforeOrAtTime(fourHoursAgo)
				.getList());
		assertEquals(staticValues, timeCollection.query()
				.afterTime(fourHoursAgo)
				.beforeOrAtTime(fourHoursAgo + 1)
				.getList());
		assertEquals(staticValues, timeCollection.query()
				.afterTime(twoHoursAgo)
				.beforeOrAtTime(twoHoursAgo + 1)
				.getList());
		assertEquals(emptyList, timeCollection.query()
				.afterTime(eightHoursAgo)
				.beforeTime(eightHoursAgo + 10)
				.getList());
		assertEquals(staticActiveValueOnly, timeCollection.query()
				.afterTime(now)
				.getList());
		assertEquals(staticActiveValueOnly, timeCollection.query()
				.afterTime(eightHoursAgo)
				.whereKeyIsActive()
				.getList());
		assertEquals(testValueAndStaticCompletedValue, timeCollection.query()
				.afterTime(eightHoursAgo)
				.whereKeyIsNotActive()
				.getList());
		
		assertTestValueIsReturnedForAllEquivalentKeys();
	}

	private void verifyEverythingWorksAfterChangingToTimeFrameKey2() throws BlueDbException {
		assertEquals(testValue, timeCollection.get(activeKey));
		assertEquals(allValues, timeCollection.query()
			.afterTime(eightHoursAgo)
			.beforeTime(now)
			.getList());
		assertEquals(allValues, timeCollection.query()
				.afterTime(fourHoursAgo)
				.beforeOrAtTime(fourHoursAgo + 1)
				.getList());
		assertEquals(staticValues, timeCollection.query()
				.afterTime(twoHoursAgo)
				.beforeOrAtTime(twoHoursAgo + 1)
				.getList());
		assertEquals(emptyList, timeCollection.query()
				.afterTime(eightHoursAgo)
				.beforeTime(eightHoursAgo + 10)
				.getList());
		assertEquals(staticActiveValueOnly, timeCollection.query()
				.afterTime(now)
				.getList());
		assertEquals(staticActiveValueOnly, timeCollection.query()
				.afterTime(eightHoursAgo)
				.whereKeyIsActive()
				.getList());
		assertEquals(testValueAndStaticCompletedValue, timeCollection.query()
				.afterTime(eightHoursAgo)
				.whereKeyIsNotActive()
				.getList());
		
		assertTestValueIsReturnedForAllEquivalentKeys();
	}

	private void verifyEverythingWorksAfterChangingToActiveTimeKey() throws BlueDbException {
		assertEquals(testValue, timeCollection.get(activeKey));
		assertEquals(allValues, timeCollection.query()
			.afterTime(eightHoursAgo)
			.beforeTime(now)
			.getList());
		assertEquals(allValues, timeCollection.query()
				.afterTime(fourHoursAgo)
				.beforeOrAtTime(fourHoursAgo + 1)
				.getList());
		assertEquals(allValues, timeCollection.query()
				.afterTime(twoHoursAgo)
				.beforeOrAtTime(twoHoursAgo + 1)
				.getList());
		assertEquals(emptyList, timeCollection.query()
				.afterTime(eightHoursAgo)
				.beforeTime(eightHoursAgo + 10)
				.getList());
		assertEquals(testValueAndStaticActiveValue, timeCollection.query()
				.afterTime(now)
				.getList());
		assertEquals(testValueAndStaticActiveValue, timeCollection.query()
				.afterTime(eightHoursAgo)
				.whereKeyIsActive()
				.getList());
		assertEquals(staticCompletedValueOnly, timeCollection.query()
				.afterTime(eightHoursAgo)
				.whereKeyIsNotActive()
				.getList());
		
		assertTestValueIsReturnedForAllEquivalentKeys();
	}

	private void verifyEverythingWorksAfterChangingToTimeKey() throws BlueDbException {
		assertEquals(testValue, timeCollection.get(activeKey));
		assertEquals(allValues, timeCollection.query()
			.afterTime(eightHoursAgo)
			.beforeTime(now)
			.getList());
		assertEquals(staticValues, timeCollection.query()
				.afterOrAtTime(fourHoursAgo - 1)
				.beforeOrAtTime(fourHoursAgo)
				.getList());
		assertEquals(staticValues, timeCollection.query()
				.afterTime(fourHoursAgo)
				.beforeOrAtTime(fourHoursAgo + 1)
				.getList());
		assertEquals(staticValues, timeCollection.query()
				.afterTime(twoHoursAgo)
				.beforeOrAtTime(twoHoursAgo + 1)
				.getList());
		assertEquals(emptyList, timeCollection.query()
				.afterTime(eightHoursAgo)
				.beforeTime(eightHoursAgo + 10)
				.getList());
		assertEquals(staticActiveValueOnly, timeCollection.query()
				.afterTime(now)
				.getList());
		assertEquals(staticActiveValueOnly, timeCollection.query()
				.afterTime(eightHoursAgo)
				.whereKeyIsActive()
				.getList());
		assertEquals(testValueAndStaticCompletedValue, timeCollection.query()
				.afterTime(eightHoursAgo)
				.whereKeyIsNotActive()
				.getList());
		
		assertTestValueIsReturnedForAllEquivalentKeys();
	}

	private void assertTestValueIsReturnedForAllEquivalentKeys() throws BlueDbException {
		assertTestValueIsReturnedForKey(timeCollection, activeKey, testValue);
		assertTestValueIsReturnedForKey(timeCollection, timeFrameKey1, testValue);
		assertTestValueIsReturnedForKey(timeCollection, timeFrameKey2, testValue);
		assertTestValueIsReturnedForKey(timeCollection, timeKey, testValue);
	}

	private void assertTestValueIsReturnedForKey(ReadWriteTimeCollectionOnDisk<TestValue> timeCollection, TimeKey key, TestValue testValue) throws BlueDbException {
		assertEquals(testValue, timeCollection.get(key));
		List<TestValue> list = timeCollection.query().whereKeyIsIn(new HashSet<>(Arrays.asList(key))).getList();
		assertEquals(1, list.size());
		assertEquals(testValue, list.get(0));
	}
	
	@FunctionalInterface
	private static interface TestTimeKeyUpdater {
		public void execute() throws BlueDbException;
	}
	
}
