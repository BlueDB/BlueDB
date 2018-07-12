package io.bluedb.disk.segment.rollup;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.segment.Range;
import io.bluedb.disk.segment.rollup.RollupScheduler;

public class RollupSchedulerTest extends BlueDbDiskTestBase {

	@Test
	public void test_reportInsert() {
		Range timeRange = new Range(2, 5);
		assertEquals(Long.MIN_VALUE, getRollupScheduler().getLastInsertTime(timeRange));
		long insertTime = System.currentTimeMillis();
		getRollupScheduler().reportInsert(timeRange, insertTime);
		assertEquals(insertTime, getRollupScheduler().getLastInsertTime(timeRange));

		getRollupScheduler().reportInsert(timeRange, insertTime - 1); // report earlier time
		assertEquals(insertTime, getRollupScheduler().getLastInsertTime(timeRange));
	}

	@Test
	public void test_getLastInsertTime() {
		Range timeRange = new Range(2, 5);
		assertEquals(Long.MIN_VALUE, getRollupScheduler().getLastInsertTime(timeRange));
		long insertTime = System.currentTimeMillis();
		getRollupScheduler().reportInsert(timeRange, insertTime);
		assertEquals(insertTime, getRollupScheduler().getLastInsertTime(timeRange));
	}

	@Test
	public void test_timeRangesReadyForRollup() {
		Range timeRange0to1 = new Range(0, 1);
		Range timeRange2to3 = new Range(2, 3);
		Range timeRange4to5 = new Range(4, 5);
		getRollupScheduler().reportInsert(timeRange0to1, 0);
		getRollupScheduler().reportInsert(timeRange2to3, 0);
		getRollupScheduler().reportInsert(timeRange2to3, System.currentTimeMillis());
		List<Range> readyForRollup = getRollupScheduler().timeRangesReadyForRollup();
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
		List<Range> rollupsRequested = new ArrayList<>();
		BlueCollectionOnDisk<TestValue> mockCollection = createMockCollection(rollupsRequested);
		RollupScheduler mockRollupScheduler = new RollupScheduler(mockCollection);
		Range timeRange = new Range(0, 1);
		assertEquals(Long.MIN_VALUE, mockRollupScheduler.getLastInsertTime(timeRange));
		mockRollupScheduler.reportInsert(timeRange, 0);
		assertEquals(0, mockRollupScheduler.getLastInsertTime(timeRange));
		mockRollupScheduler.scheduleReadyRollups();

		assertEquals(1, rollupsRequested.size());
		assertTrue(rollupsRequested.contains(timeRange));
	}

	@Test
	public void test_run() {
		List<Range> rollupsRequested = new ArrayList<>();
		BlueCollectionOnDisk<TestValue> mockCollection = createMockCollection(rollupsRequested);
		RollupScheduler mockRollupScheduler = new RollupScheduler(mockCollection);
		Range timeRange = new Range(0, 1);
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

	private BlueCollectionOnDisk<TestValue> createMockCollection(List<Range> rollupsRequested) {
		return new BlueCollectionOnDisk<TestValue>(db(), "test_RollupSchedulerTest", TestValue.class) {
			@Override
			public void scheduleRollup(Range t) {
				rollupsRequested.add(t);
			}
		};
	}
}
