package org.bluedb.disk;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.disk.collection.ReadWriteTimeCollectionOnDisk;
import org.bluedb.disk.helpers.BlueDbOnDiskWrapper;
import org.bluedb.disk.helpers.BlueDbOnDiskWrapper.StartupOption;
import org.junit.Test;

public class WhereKeyIsInIntegrationTest {
	@Test
	public void test_whereKeyIsIn() throws IOException, BlueDbException {
		try (BlueDbOnDiskWrapper dbWrapper = new BlueDbOnDiskWrapper(StartupOption.EncryptionDisabled, null)) {
			long oneHour = TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
			long now = System.currentTimeMillis();
			long oneHourAgo = now - oneHour;
			long threeHoursAgo = now - oneHour*3;
			long fourHoursAgo = now - oneHour*4;
			long fiveHoursAgo = now - oneHour*5;
			long sixHoursAgo = now - oneHour*6;
			long sevenHoursAgo = now - oneHour*7;
			long eightHoursAgo = now - oneHour*8;

			ArrayList<BlueKey> keys = new ArrayList<>();
			keys.add(new TimeFrameKey(0, eightHoursAgo, sixHoursAgo));
			keys.add(new TimeFrameKey(1, eightHoursAgo, fiveHoursAgo));
			keys.add(new TimeFrameKey(2, eightHoursAgo + 10, fourHoursAgo));
			keys.add(new TimeFrameKey(3, sevenHoursAgo, now));
			keys.add(new TimeFrameKey(4, fourHoursAgo, fourHoursAgo + 10));
			keys.add(new TimeFrameKey(5, threeHoursAgo, oneHourAgo + 10));
			
			Set<BlueKey> fullKeySet = new HashSet<>(keys);
			Set<BlueKey> keySet0 = new HashSet<>(Arrays.asList(keys.get(0)));
			Set<BlueKey> keySet1 = new HashSet<>(Arrays.asList(keys.get(1)));
			Set<BlueKey> keySet2 = new HashSet<>(Arrays.asList(keys.get(2)));
			Set<BlueKey> keySet3 = new HashSet<>(Arrays.asList(keys.get(3)));
			Set<BlueKey> keySet4 = new HashSet<>(Arrays.asList(keys.get(4)));
			Set<BlueKey> keySet5 = new HashSet<>(Arrays.asList(keys.get(5)));
			Set<BlueKey> keySet1_5 = new HashSet<>(Arrays.asList(keys.get(1), keys.get(5)));
			Set<BlueKey> keySet2_3 = new HashSet<>(Arrays.asList(keys.get(2), keys.get(3)));
			Set<BlueKey> keySet2_4 = new HashSet<>(Arrays.asList(keys.get(2), keys.get(4)));
			Set<BlueKey> keySet0_3 = new HashSet<>(Arrays.asList(keys.get(0), keys.get(3)));
			Set<BlueKey> keySet0_3_5 = new HashSet<>(Arrays.asList(keys.get(0), keys.get(3), keys.get(5)));
			
			Map<BlueKey, TestValue> keyValuePairs = new HashMap<>();
			for(int i = 0; i < 6; i++) {
				keyValuePairs.put(keys.get(i), new TestValue(String.valueOf(i), i));
			}
			
			ReadWriteTimeCollectionOnDisk<TestValue> timeCollection = dbWrapper.getTimeCollection();
			
			List<TestValue> queryResults = timeCollection.query()
					.whereKeyIsIn(fullKeySet)
					.getList();
			assertQueryResults(queryResults, 0);
			
			timeCollection.batchUpsert(keyValuePairs);
			
			queryResults = timeCollection.query()
				.whereKeyIsIn(fullKeySet)
				.getList();
			assertQueryResults(queryResults, 0, 0, 1, 2, 3, 4, 5);
			
			queryResults = timeCollection.query()
					.whereKeyIsIn(keySet0)
					.getList();
			assertQueryResults(queryResults, 0, 0);
			
			queryResults = timeCollection.query()
					.whereKeyIsIn(keySet1)
					.getList();
			assertQueryResults(queryResults, 0, 1);
			
			queryResults = timeCollection.query()
					.whereKeyIsIn(keySet2)
					.getList();
			assertQueryResults(queryResults, 0, 2);
			
			queryResults = timeCollection.query()
					.whereKeyIsIn(keySet3)
					.getList();
			assertQueryResults(queryResults, 0, 3);
			
			queryResults = timeCollection.query()
					.whereKeyIsIn(keySet4)
					.getList();
			assertQueryResults(queryResults, 0, 4);
			
			queryResults = timeCollection.query()
					.whereKeyIsIn(keySet5)
					.getList();
			assertQueryResults(queryResults, 0, 5);
			
			queryResults = timeCollection.query()
					.whereKeyIsIn(keySet1_5)
					.getList();
			assertQueryResults(queryResults, 0, 1, 5);
			
			queryResults = timeCollection.query()
					.whereKeyIsIn(keySet2_4)
					.getList();
			assertQueryResults(queryResults, 0, 2, 4);
			
			queryResults = timeCollection.query()
					.whereKeyIsIn(keySet0_3)
					.getList();
			assertQueryResults(queryResults, 0, 0, 3);
			
			queryResults = timeCollection.query()
					.whereKeyIsIn(keySet0_3_5)
					.getList();
			assertQueryResults(queryResults, 0, 0, 3, 5);
			
			//Verify that we can find keys that start before the timeframe but do overlap it.
			queryResults = timeCollection.query()
					.whereKeyIsIn(keySet2_3)
					.afterOrAtTime(sevenHoursAgo)
					.beforeTime(now)
					.getList();
			assertQueryResults(queryResults, 0, 2, 3);
			
			//Verify that keys in the whereKeyIsIn that don't match other criteria are still ignored
			queryResults = timeCollection.query()
					.whereKeyIsIn(keySet2_3)
					.afterOrAtTime(threeHoursAgo)
					.beforeTime(now)
					.getList();
			assertQueryResults(queryResults, 0, 3);
		}
	}

	private void assertQueryResults(List<TestValue> actualValues, int cupcakeModifier, int...expectedValueIndicies) {
		List<TestValue> expectedValues = new LinkedList<>();
		for(int i = 0; i < expectedValueIndicies.length; i++) {
			int expectedIndex = expectedValueIndicies[i];
			expectedValues.add(new TestValue(String.valueOf(expectedIndex), expectedIndex + cupcakeModifier));
		}
		assertEquals(expectedValues, actualValues);
	}
}
