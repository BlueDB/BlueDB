package org.bluedb.disk;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.disk.collection.ReadWriteTimeCollectionOnDisk;
import org.bluedb.disk.collection.index.TestRetrievalKeyExtractor;
import org.bluedb.disk.helpers.BlueDbOnDiskWrapper;
import org.bluedb.disk.helpers.BlueDbOnDiskWrapper.StartupOption;
import org.junit.Test;

public class BatchUpdateIntegrationTest {
	@Test
	public void test_largeBatchUpsert() throws IOException, BlueDbException {
		try (BlueDbOnDiskWrapper dbWrapper = new BlueDbOnDiskWrapper(StartupOption.EncryptionDisabled)) {
			long now = System.currentTimeMillis();
			long oneHour = TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
			int valuesToInsert = 2000;
			
			Map<BlueKey, TestValue> recordsToInsert = new HashMap<>();
			for(int i = 0; i < valuesToInsert; i++) {
				long startTime = now - (oneHour * i);
				long endTime = startTime + oneHour;
				BlueKey key = new TimeFrameKey(i, startTime, endTime);
				TestValue value = new TestValue(String.valueOf(i), i);
				recordsToInsert.put(key, value);
			}
			
			ReadWriteTimeCollectionOnDisk<TestValue> timeCollection = dbWrapper.getTimeCollection();
			BlueIndex<IntegerKey, TestValue> cookieIndex = timeCollection.createIndex("COOKIE_INDEX", IntegerKey.class, new TestRetrievalKeyExtractor());
			
			List<TestValue> queryResults = timeCollection.query()
					.getList();
			assertQueryResults(queryResults, 0);
			
			timeCollection.batchUpsert(recordsToInsert);
			
			queryResults = timeCollection.query()
					.getList();
			assertEquals(valuesToInsert, queryResults.size());
			
			int targetCookieCount = 0;
			queryResults = cookieIndex.get(new IntegerKey(targetCookieCount));
			assertEquals(1, queryResults.size());
			assertEquals(String.valueOf(0), queryResults.get(0).getName());
			assertEquals(targetCookieCount, queryResults.get(0).getCupcakes());
			
			timeCollection.query()
				.update(value -> value.addCupcake());
			
			targetCookieCount = 1;
			queryResults = cookieIndex.get(new IntegerKey(targetCookieCount));
			assertEquals(1, queryResults.size());
			assertEquals(String.valueOf(0), queryResults.get(0).getName());
			assertEquals(targetCookieCount, queryResults.get(0).getCupcakes());
		}
	}
	
	@Test
	public void test_smallBatchUpsert() throws IOException, BlueDbException {
		try (BlueDbOnDiskWrapper dbWrapper = new BlueDbOnDiskWrapper(StartupOption.EncryptionDisabled)) {
			long oneHour = TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
			long now = System.currentTimeMillis();
			long oneHourAgo = now - oneHour;
			long twoHoursAgo = now - oneHour*2;
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
			
			Map<BlueKey, TestValue> keyValuePairs = new HashMap<>();
			for(int i = 0; i < 6; i++) {
				keyValuePairs.put(keys.get(i), new TestValue(String.valueOf(i), i));
			}
			
			ReadWriteTimeCollectionOnDisk<TestValue> timeCollection = dbWrapper.getTimeCollection();
			
			List<TestValue> queryResults = timeCollection.query()
					.getList();
			assertQueryResults(queryResults, 0);
			
			timeCollection.batchUpsert(keyValuePairs);
			
			queryResults = timeCollection.query()
				.getList();
			assertQueryResults(queryResults, 0, 0, 1, 2, 3, 4, 5);
			
			queryResults = timeCollection.query()
					.afterTime(eightHoursAgo - 10)
					.beforeTime(now + 10)
					.getList();
			assertQueryResults(queryResults, 0, 0, 1, 2, 3, 4, 5);
			
			queryResults = timeCollection.query()
					.afterTime(sevenHoursAgo)
					.beforeTime(fourHoursAgo)
					.getList();
			assertQueryResults(queryResults, 0, 0, 1, 2, 3);
			
			queryResults = timeCollection.query()
					.afterTime(fiveHoursAgo)
					.beforeTime(threeHoursAgo)
					.getList();
			assertQueryResults(queryResults, 0, 2, 3, 4);
			
			queryResults = timeCollection.query()
					.afterTime(fiveHoursAgo)
					.beforeTime(threeHoursAgo)
					.getList();
			assertQueryResults(queryResults, 0, 2, 3, 4);
			
			queryResults = timeCollection.query()
					.afterTime(fiveHoursAgo)
					.beforeTime(twoHoursAgo)
					.getList();
			assertQueryResults(queryResults, 0, 2, 3, 4, 5);
			
			queryResults = timeCollection.query()
					.afterTime(fiveHoursAgo)
					.beforeTime(twoHoursAgo)
					.byStartTime()
					.getList();
			assertQueryResults(queryResults, 0, 4, 5);
			
			timeCollection.query()
					.afterTime(fiveHoursAgo)
					.beforeTime(twoHoursAgo)
					.update(value -> value.addCupcake());
			queryResults = timeCollection.query()
					.afterTime(fiveHoursAgo)
					.beforeTime(twoHoursAgo)
					.getList();
			assertQueryResults(queryResults, 1, 2, 3, 4, 5);
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
