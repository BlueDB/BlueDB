package org.bluedb.disk;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.bluedb.api.BlueTimeCollection;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.exceptions.UnsupportedIndexConditionTypeException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.BlueIndexInfo;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.collection.index.TestEndTimeIndexKeyExtractor;
import org.bluedb.disk.collection.index.TestStartTimeIndexKeyExtractor;
import org.bluedb.disk.helpers.BlueDbOnDiskWrapper;
import org.bluedb.disk.helpers.BlueDbOnDiskWrapper.StartupOption;
import org.junit.Test;

public class StartAndEndTimeIndexConditionIntegrationTest {
	@Test
	public void test_startAndEndTimeIndexConditionQueries() throws IOException, BlueDbException {
		try (BlueDbOnDiskWrapper dbWrapper = new BlueDbOnDiskWrapper(StartupOption.EncryptionDisabled, null)) {
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

			ArrayList<TimeFrameKey> keys = new ArrayList<>();
			keys.add(new TimeFrameKey(1L, eightHoursAgo, sixHoursAgo));
			keys.add(new TimeFrameKey(2L, eightHoursAgo, fiveHoursAgo));
			keys.add(new TimeFrameKey(3L, eightHoursAgo + 10, fourHoursAgo));
			keys.add(new TimeFrameKey(4L, sevenHoursAgo, now));
			keys.add(new TimeFrameKey(5L, fourHoursAgo, fourHoursAgo + 10));
			keys.add(new TimeFrameKey(6L, threeHoursAgo, oneHourAgo + 10));

			Map<BlueKey, TestValueWithTimes> keyValuePairs = new HashMap<>();
			for(int i = 0; i < 6; i++) {
				TimeFrameKey key = keys.get(i);
				keyValuePairs.put(key, createTestValueFromKey(key, i, 0));
			}

			BlueTimeCollection<TestValueWithTimes> timeCollection2 = dbWrapper.getDb().getTimeCollectionBuilder("time-collection-2", TimeFrameKey.class, TestValueWithTimes.class).build();

			List<BlueIndexInfo<? extends ValueKey,TestValueWithTimes>> indexInfo = new LinkedList<BlueIndexInfo<? extends ValueKey,TestValueWithTimes>>();

			String startTimeIndexName = "start-time-index";
			indexInfo.add(new BlueIndexInfo<LongKey, TestValueWithTimes>(startTimeIndexName, LongKey.class, new TestStartTimeIndexKeyExtractor()));

			String endTimeIndexName = "end-time-index";
			indexInfo.add(new BlueIndexInfo<LongKey, TestValueWithTimes>(endTimeIndexName, LongKey.class, new TestEndTimeIndexKeyExtractor()));

			timeCollection2.createIndices(indexInfo);

			BlueIndex<LongKey, TestValueWithTimes> startTimeIndex = timeCollection2.getIndex(startTimeIndexName, LongKey.class);
			BlueIndex<LongKey, TestValueWithTimes> endTimeIndex = timeCollection2.getIndex(endTimeIndexName, LongKey.class);

			List<TestValueWithTimes> queryResults = getValuesInTimeframe(timeCollection2, startTimeIndex, endTimeIndex, eightHoursAgo, now);
			assertQueryResults(queryResults, keys, 0);

			timeCollection2.batchUpsert(keyValuePairs);

			queryResults = getValuesInTimeframe(timeCollection2, startTimeIndex, endTimeIndex, eightHoursAgo, now);
			assertQueryResults(queryResults, keys, 0, 0, 1, 2, 3, 4, 5);

			queryResults = getValuesInTimeframe(timeCollection2, startTimeIndex, endTimeIndex, eightHoursAgo, sevenHoursAgo);
			assertQueryResults(queryResults, keys, 0, 0, 1, 2);

			queryResults = getValuesInTimeframe(timeCollection2, startTimeIndex, endTimeIndex, sevenHoursAgo, sixHoursAgo);
			assertQueryResults(queryResults, keys, 0, 0, 1, 2, 3);

			queryResults = getValuesInTimeframe(timeCollection2, startTimeIndex, endTimeIndex, sixHoursAgo, fiveHoursAgo);
			assertQueryResults(queryResults, keys, 0, 0, 1, 2, 3);

			queryResults = getValuesInTimeframe(timeCollection2, startTimeIndex, endTimeIndex, fiveHoursAgo, fourHoursAgo);
			assertQueryResults(queryResults, keys, 0, 1, 2, 3);

			queryResults = getValuesInTimeframe(timeCollection2, startTimeIndex, endTimeIndex, fourHoursAgo, threeHoursAgo);
			assertQueryResults(queryResults, keys, 0, 2, 3, 4);

			queryResults = getValuesInTimeframe(timeCollection2, startTimeIndex, endTimeIndex, threeHoursAgo, twoHoursAgo);
			assertQueryResults(queryResults, keys, 0, 3, 5);

			queryResults = getValuesInTimeframe(timeCollection2, startTimeIndex, endTimeIndex, twoHoursAgo, oneHourAgo);
			assertQueryResults(queryResults, keys, 0, 3, 5);

			queryResults = getValuesInTimeframe(timeCollection2, startTimeIndex, endTimeIndex, oneHourAgo, now);
			assertQueryResults(queryResults, keys, 0, 3, 5);

			timeCollection2.query()
				.where(endTimeIndex.createLongIndexCondition().isGreaterThanOrEqualTo(oneHourAgo))
				.where(startTimeIndex.createLongIndexCondition().isLessThan(now))
				.update(TestValueWithTimes::addCupcake);

			queryResults = getValuesInTimeframe(timeCollection2, startTimeIndex, endTimeIndex, oneHourAgo, now);
			assertQueryResults(queryResults, keys, 1, 3, 5);
		}
	}

	private List<TestValueWithTimes> getValuesInTimeframe(BlueTimeCollection<TestValueWithTimes> collection,
			BlueIndex<LongKey, TestValueWithTimes> startTimeIndex, BlueIndex<LongKey, TestValueWithTimes> endTimeIndex,
			long start, long end) throws BlueDbException, UnsupportedIndexConditionTypeException {
		return collection.query()
				.where(endTimeIndex.createLongIndexCondition().isGreaterThanOrEqualTo(start))
				.where(startTimeIndex.createLongIndexCondition().isLessThan(end))
				.getList();
	}

	private void assertQueryResults(List<TestValueWithTimes> actualValues, ArrayList<TimeFrameKey> keys, int cupcakeModifier, int...expectedValueIndicies) {
		List<TestValueWithTimes> expectedValues = new LinkedList<>();
		for(int i = 0; i < expectedValueIndicies.length; i++) {
			int expectedIndex = expectedValueIndicies[i];
			expectedValues.add(createTestValueFromKey(keys.get(expectedIndex), expectedIndex, cupcakeModifier));
		}
		assertEquals(expectedValues, actualValues);
	}

	private TestValueWithTimes createTestValueFromKey(TimeFrameKey key, int index, int cupcakeModifier) {
		return new TestValueWithTimes(key.getId().getLongIdIfPresent(), key.getStartTime(), key.getEndTime(), String.valueOf(index), index + cupcakeModifier);
	}
}
