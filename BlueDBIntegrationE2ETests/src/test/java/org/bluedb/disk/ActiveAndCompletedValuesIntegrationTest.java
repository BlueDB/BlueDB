package org.bluedb.disk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.bluedb.api.BlueCollectionVersion;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.ActiveTimeKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.collection.ReadWriteTimeCollectionOnDisk;
import org.bluedb.disk.helpers.BlueDbOnDiskWrapper;
import org.bluedb.disk.helpers.BlueDbOnDiskWrapper.StartupOption;
import org.junit.Test;

public class ActiveAndCompletedValuesIntegrationTest {
	private long oneHour = TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
	private long now = System.currentTimeMillis();
	private long oneHourAgo = now - oneHour;
//	private long twoHoursAgo = now - oneHour*2;
//	private long threeHoursAgo = now - oneHour*3;
//	private long fourHoursAgo = now - oneHour*4;
//	private long fiveHoursAgo = now - oneHour*5;
//	private long sixHoursAgo = now - oneHour*6;
//	private long sevenHoursAgo = now - oneHour*7;
	private long eightHoursAgo = now - oneHour*8;
	
	@Test
	public void test_activeAndCompletedValuesInSameCollection() throws IOException, BlueDbException {
		try (BlueDbOnDiskWrapper dbWrapper = new BlueDbOnDiskWrapper(StartupOption.EncryptionDisabled, BlueCollectionVersion.VERSION_2)) {
			
			ReadWriteTimeCollectionOnDisk<TestValue> timeCollection = dbWrapper.getTimeCollection();
			
			UUID id = UUID.randomUUID();
			long start = eightHoursAgo;
			TestValue value = new TestValue("Bob", 365);
			
			ArrayList<TimeKey> equivalentTimeKeys = new ArrayList<>();
			equivalentTimeKeys.add(new TimeKey(id, start));
			equivalentTimeKeys.add(new TimeFrameKey(id, start, oneHourAgo));
			equivalentTimeKeys.add(new TimeFrameKey(id, start, now));
			equivalentTimeKeys.add(new ActiveTimeKey(id, start));
			
			for(int i = 0; i < equivalentTimeKeys.size(); i++) {
				verifyAccessWorksWithEachEquivalentTimeKey(timeCollection, equivalentTimeKeys.get(i), equivalentTimeKeys, value);
				verifyValuesAreConsideredDuplicates(timeCollection, equivalentTimeKeys);
				timeCollection.delete(equivalentTimeKeys.get((i + 1) % equivalentTimeKeys.size()));
			}
		}
	}

	private void verifyAccessWorksWithEachEquivalentTimeKey(ReadWriteTimeCollectionOnDisk<TestValue> timeCollection, TimeKey timeKeyToInsert, 
			ArrayList<TimeKey> equivalentTimeKeysToCheck, TestValue value) throws BlueDbException {
		timeCollection.insert(timeKeyToInsert, value);
		for(TimeKey timeKey : equivalentTimeKeysToCheck) {
			assertEquals(value, timeCollection.get(timeKey));
			assertEquals(value, timeCollection.query()
				.whereKeyIsIn(new HashSet<>(Arrays.asList(timeKey)))
				.getList()
				.get(0));
		}
	}

	private void verifyValuesAreConsideredDuplicates(ReadWriteTimeCollectionOnDisk<TestValue> timeCollection,
			ArrayList<TimeKey> equivalentTimeKeys) {
		for(TimeKey timeKey : equivalentTimeKeys) {
			try {
				timeCollection.insert(timeKey, new TestValue("Whatever"));
				fail("This should fail since it will be seen as a duplicate of an already existing key/value pair.");
			} catch (BlueDbException e) {
				//Expected
			}
		}
	}
	
}
