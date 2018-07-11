package io.bluedb.disk.segment;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.collection.BlueCollectionImpl;

public class RollupSchedulerTest extends BlueDbDiskTestBase {

	@Test
	public void test_reportInsert() {
		TimeRange timeRange = new TimeRange(2, 5);
		assertEquals(Long.MIN_VALUE, getRollupScheduler().getLastInsertTime(timeRange));
		long insertTime = System.currentTimeMillis();
		getRollupScheduler().reportInsert(timeRange, insertTime);
		assertEquals(insertTime, getRollupScheduler().getLastInsertTime(timeRange));

		getRollupScheduler().reportInsert(timeRange, insertTime - 1); // report earlier time
		assertEquals(insertTime, getRollupScheduler().getLastInsertTime(timeRange));
	}

	@Test
	public void test_getLastInsertTime() {
		TimeRange timeRange = new TimeRange(2, 5);
		assertEquals(Long.MIN_VALUE, getRollupScheduler().getLastInsertTime(timeRange));
		long insertTime = System.currentTimeMillis();
		getRollupScheduler().reportInsert(timeRange, insertTime);
		assertEquals(insertTime, getRollupScheduler().getLastInsertTime(timeRange));
	}

	@Test
	public void test_timeRangesReadyForRollup() {
		TimeRange timeRange0to1 = new TimeRange(0, 1);
		TimeRange timeRange2to3 = new TimeRange(2, 3);
		TimeRange timeRange4to5 = new TimeRange(4, 5);
		getRollupScheduler().reportInsert(timeRange0to1, 0);
		getRollupScheduler().reportInsert(timeRange2to3, 0);
		getRollupScheduler().reportInsert(timeRange2to3, System.currentTimeMillis());
		List<TimeRange> readyForRollup = getRollupScheduler().timeRangesReadyForRollup();
		assertEquals(1, readyForRollup.size());
		assertTrue(readyForRollup.contains(timeRange0to1));
	}

	@Test
	public void test_isReadyForRollup() {
		assertTrue(RollupScheduler.isReadyForRollup(0));
		assertFalse(RollupScheduler.isReadyForRollup(System.currentTimeMillis()));
	}

	@Test
	public void test_scheduleReadyRollups() {
		List<TimeRange> rollupsRequested = new ArrayList<>();
		BlueCollectionImpl<TestValue> mockCollection = createMockCollection(rollupsRequested);
		RollupScheduler mockRollupScheduler = new RollupScheduler(mockCollection);
		TimeRange timeRange = new TimeRange(0, 1);
		assertEquals(Long.MIN_VALUE, mockRollupScheduler.getLastInsertTime(timeRange));
		mockRollupScheduler.reportInsert(timeRange, 0);
		assertEquals(0, mockRollupScheduler.getLastInsertTime(timeRange));
		mockRollupScheduler.scheduleReadyRollups();

		assertEquals(1, rollupsRequested.size());
		assertTrue(rollupsRequested.contains(timeRange));
	}

	@Test
	public void test_run() {
		List<TimeRange> rollupsRequested = new ArrayList<>();
		BlueCollectionImpl<TestValue> mockCollection = createMockCollection(rollupsRequested);
		RollupScheduler mockRollupScheduler = new RollupScheduler(mockCollection);
		TimeRange timeRange = new TimeRange(0, 1);
		assertEquals(Long.MIN_VALUE, mockRollupScheduler.getLastInsertTime(timeRange));
		mockRollupScheduler.reportInsert(timeRange, 0);
		assertEquals(0, mockRollupScheduler.getLastInsertTime(timeRange));

		Thread rollupSchedulerThread = new Thread(mockRollupScheduler);
		rollupSchedulerThread.start();

		for (int i = 0; i < 100; i++) {
			try { Thread.sleep(10);} catch (InterruptedException e) {e.printStackTrace();}
			if(rollupsRequested.size() > 0)
				break;
		}
		
		rollupSchedulerThread.stop();
//		getRollupScheduler().stop();

		assertEquals(1, rollupsRequested.size());
		assertTrue(rollupsRequested.contains(timeRange));
	}

	private BlueCollectionImpl<TestValue> createMockCollection(List<TimeRange> rollupsRequested) {
		return new BlueCollectionImpl<TestValue>(db(), "test_RollupSchedulerTest", TestValue.class) {
			@Override
			public void scheduleRollup(TimeRange t) {
				rollupsRequested.add(t);
			}
		};
	}
}
