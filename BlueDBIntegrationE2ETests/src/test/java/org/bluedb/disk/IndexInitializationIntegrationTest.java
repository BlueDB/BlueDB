package org.bluedb.disk;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndexInfo;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.collection.ReadWriteTimeCollectionOnDisk;
import org.bluedb.disk.collection.index.ReadWriteIndexOnDisk;
import org.bluedb.disk.collection.index.TestMultiRetrievalKeyExtractor;
import org.bluedb.disk.collection.index.TestRetrievalKeyExtractor;
import org.bluedb.disk.helpers.BlueDbOnDiskWrapper;
import org.bluedb.disk.helpers.BlueDbOnDiskWrapper.StartupOption;
import org.junit.Test;

public class IndexInitializationIntegrationTest {
	@Test
	public void test_largeIndexInitialization() throws IOException, BlueDbException {
		try (BlueDbOnDiskWrapper dbWrapper = new BlueDbOnDiskWrapper(StartupOption.EncryptionDisabled)) {
			long now = System.currentTimeMillis();
			long oneHour = TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
			int valuesToInsert = 5_000;
			
			Map<BlueKey, TestValue> recordsToInsert = new HashMap<>();
			for(int i = 0; i < valuesToInsert; i++) {
				long startTime = now - (oneHour * i);
				long endTime = startTime + oneHour * 5;
				BlueKey key = new TimeFrameKey(i, startTime, endTime);
				TestValue value = new TestValue(String.valueOf(i), i);
				recordsToInsert.put(key, value);
			}
			
			ReadWriteTimeCollectionOnDisk<TestValue> timeCollection = dbWrapper.getTimeCollection();
			
			List<TestValue> queryResults = timeCollection.query()
					.getList();
			assertQueryResults(queryResults, 0);
			
			Instant insertionStartTime = Instant.now();
			timeCollection.batchUpsert(recordsToInsert);
			System.out.println("Record Insertion Time (" + valuesToInsert + " records):" + Duration.between(insertionStartTime, Instant.now()));
			
			queryResults = timeCollection.query()
					.getList();
			assertEquals(valuesToInsert, queryResults.size());
			
			Instant indexCreationStartTime = Instant.now();
			List<BlueIndexInfo<? extends ValueKey, TestValue>> indexInfoList = new LinkedList<>();
			indexInfoList.add(new BlueIndexInfo<IntegerKey, TestValue>("COOKIE_INDEX", IntegerKey.class, new TestRetrievalKeyExtractor()));
			indexInfoList.add(new BlueIndexInfo<IntegerKey, TestValue>("COOKIE_INDEX2", IntegerKey.class, new TestMultiRetrievalKeyExtractor()));
			timeCollection.createIndices(indexInfoList);
			System.out.println("Index Creation Time (" + 2 + " indices " + valuesToInsert + " records):" + Duration.between(indexCreationStartTime, Instant.now()));
			
			ReadWriteIndexOnDisk<IntegerKey, TestValue> index1 = timeCollection.getIndex("COOKIE_INDEX", IntegerKey.class);
			ReadWriteIndexOnDisk<IntegerKey, TestValue> index2 = timeCollection.getIndex("COOKIE_INDEX2", IntegerKey.class);
			
			int targetCookieCount = new Random().nextInt(valuesToInsert-2);
			queryResults = timeCollection.query()
					.where(index1.createIntegerIndexCondition().isEqualTo(targetCookieCount))
					.getList();
			assertEquals(1, queryResults.size());
			assertEquals(String.valueOf(targetCookieCount), queryResults.get(0).getName());
			assertEquals(targetCookieCount, queryResults.get(0).getCupcakes());
			
			queryResults = timeCollection.query()
					.where(index2.createIntegerIndexCondition().isEqualTo(targetCookieCount))
					.getList();
			assertEquals(2, queryResults.size());
			assertEquals(String.valueOf(targetCookieCount), queryResults.get(0).getName());
			assertEquals(targetCookieCount, queryResults.get(0).getCupcakes());
			assertEquals(String.valueOf(targetCookieCount - 2), queryResults.get(1).getName());
			assertEquals(targetCookieCount - 2, queryResults.get(1).getCupcakes());
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
