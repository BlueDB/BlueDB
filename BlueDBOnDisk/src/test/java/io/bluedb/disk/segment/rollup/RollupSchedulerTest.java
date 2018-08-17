package io.bluedb.disk.segment.rollup;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.segment.Range;
import io.bluedb.disk.segment.rollup.RollupScheduler;

public class RollupSchedulerTest extends BlueDbDiskTestBase {

	@Test
	public void test_reportInsert() {
		Range timeRange = new Range(2, 5);
		RollupTarget rollupTarget = new RollupTarget(0, timeRange);
		assertEquals(Long.MIN_VALUE, getRollupScheduler().getLastInsertTime(rollupTarget));
		long insertTime = System.currentTimeMillis();
		getRollupScheduler().reportInsert(rollupTarget, insertTime);
		assertEquals(insertTime, getRollupScheduler().getLastInsertTime(rollupTarget));

		getRollupScheduler().reportInsert(rollupTarget, insertTime - 1); // report earlier time
		assertEquals(insertTime, getRollupScheduler().getLastInsertTime(rollupTarget));
	}

	@Test
	public void test_getLastInsertTime() {
		Range timeRange = new Range(2, 5);
		RollupTarget rollupTarget = new RollupTarget(0, timeRange);
		assertEquals(Long.MIN_VALUE, getRollupScheduler().getLastInsertTime(rollupTarget));
		long insertTime = System.currentTimeMillis();
		getRollupScheduler().reportInsert(rollupTarget, insertTime);
		assertEquals(insertTime, getRollupScheduler().getLastInsertTime(rollupTarget));
	}

	@Test
	public void test_timeRangesReadyForRollup() {
		Range timeRange0to1 = new Range(0, 1);
		Range timeRange2to3 = new Range(2, 3);
		RollupTarget rollupTarget0to1 = new RollupTarget(0, timeRange0to1);
		RollupTarget rollupTarget2to3 = new RollupTarget(0, timeRange2to3);
		getRollupScheduler().reportInsert(rollupTarget0to1, 0);
		getRollupScheduler().reportInsert(rollupTarget2to3, 0);
		getRollupScheduler().reportInsert(rollupTarget2to3, System.currentTimeMillis());
		List<RollupTarget> readyForRollup = getRollupScheduler().rollupTargetsReadyForRollup();
		assertEquals(1, readyForRollup.size());
		assertTrue(readyForRollup.contains(rollupTarget0to1));
	}

	@Test
	public void test_isReadyForRollup() {
		assertTrue(RollupScheduler.isReadyForRollup(0));
		assertFalse(RollupScheduler.isReadyForRollup(System.currentTimeMillis()));
	}

	@Test
	public void test_scheduleReadyRollups() throws Exception {
		List<RollupTarget> rollupsRequested = new ArrayList<>();
		BlueCollectionOnDisk<TestValue> mockCollection = createMockCollection(rollupsRequested);
		RollupScheduler mockRollupScheduler = new RollupScheduler(mockCollection);
		Range timeRange = new Range(0, 1);
		RollupTarget rollupTarget = new RollupTarget(0, timeRange);
		assertEquals(Long.MIN_VALUE, mockRollupScheduler.getLastInsertTime(rollupTarget));
		mockRollupScheduler.reportInsert(rollupTarget, 0);
		assertEquals(0, mockRollupScheduler.getLastInsertTime(rollupTarget));
		mockRollupScheduler.scheduleReadyRollups();

		assertEquals(1, rollupsRequested.size());
		assertTrue(rollupsRequested.contains(rollupTarget));
	}

	@Test
	public void test_forceScheduleRollups() throws Exception {
		List<RollupTarget> rollupsRequested = new ArrayList<>();
		BlueCollectionOnDisk<TestValue> mockCollection = createMockCollection(rollupsRequested);
		RollupScheduler mockRollupScheduler = new RollupScheduler(mockCollection);
		Range timeRange = new Range(0, 1);
		RollupTarget rollupTarget = new RollupTarget(0, timeRange);
		assertEquals(Long.MIN_VALUE, mockRollupScheduler.getLastInsertTime(rollupTarget));

		long now = System.currentTimeMillis();
		mockRollupScheduler.reportInsert(rollupTarget, now);
		assertEquals(now, mockRollupScheduler.getLastInsertTime(rollupTarget));

		mockRollupScheduler.scheduleReadyRollups();
		assertEquals(0, rollupsRequested.size());

		mockRollupScheduler.forceScheduleRollups();
		assertEquals(1, rollupsRequested.size());
		assertTrue(rollupsRequested.contains(rollupTarget));
	}

	@Test
	public void test_run() throws Exception {
		List<RollupTarget> rollupsRequested = new ArrayList<>();
		BlueCollectionOnDisk<TestValue> mockCollection = createMockCollection(rollupsRequested);
		RollupScheduler mockRollupScheduler = new RollupScheduler(mockCollection);
		Range timeRange = new Range(0, 1);
		RollupTarget rollupTarget = new RollupTarget(0, timeRange);
		assertEquals(Long.MIN_VALUE, mockRollupScheduler.getLastInsertTime(rollupTarget));
		mockRollupScheduler.reportInsert(rollupTarget, 0);
		assertEquals(0, mockRollupScheduler.getLastInsertTime(rollupTarget));

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
		assertTrue(rollupsRequested.contains(rollupTarget));
	}

	@Test
	public void test_run_interruption() {
		RollupScheduler mockRollupScheduler = new RollupScheduler(getTimeCollection());
		mockRollupScheduler.setWaitBetweenReviews(0);
		Range timeRange = new Range(0, 1);
		RollupTarget rollupTarget = new RollupTarget(0, timeRange);
		assertEquals(Long.MIN_VALUE, mockRollupScheduler.getLastInsertTime(rollupTarget));


		Thread rollupSchedulerThread = new Thread(mockRollupScheduler);
		rollupSchedulerThread.start();
		Blutils.trySleep(10);
		assertTrue(mockRollupScheduler.isRunning());
		rollupSchedulerThread.interrupt();
		Blutils.trySleep(20);
		assertFalse(mockRollupScheduler.isRunning());
	}

	private BlueCollectionOnDisk<TestValue> createMockCollection(List<RollupTarget> rollupsRequested) throws Exception {
        return new BlueCollectionOnDisk<TestValue>(db(), "test_RollupSchedulerTest", TimeKey.class, TestValue.class) {
            @Override
            public void scheduleRollup(RollupTarget t) {
                rollupsRequested.add(t);
            }
        };
	}
}
