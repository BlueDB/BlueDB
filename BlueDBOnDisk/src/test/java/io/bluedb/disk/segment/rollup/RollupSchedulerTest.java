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
	public void test_reportWrite() {
		Range timeRange = new Range(2, 5);
		RollupTarget rollupTarget = new RollupTarget(0, timeRange);
		assertEquals(Long.MAX_VALUE, getRollupScheduler().getScheduledRollupTime(rollupTarget));
		long insertTime = System.currentTimeMillis();
		getRollupScheduler().reportWrite(rollupTarget, insertTime);
		assertEquals(insertTime + RollupScheduler.WAIT_AFTER_WRITE_BEFORE_ROLLUP, getRollupScheduler().getScheduledRollupTime(rollupTarget));

		getRollupScheduler().reportWrite(rollupTarget, insertTime - 1); // report earlier time
		assertEquals(insertTime + RollupScheduler.WAIT_AFTER_WRITE_BEFORE_ROLLUP, getRollupScheduler().getScheduledRollupTime(rollupTarget));
	}

	@Test
	public void test_getLastWriteTime() {
		Range timeRange = new Range(2, 5);
		RollupTarget rollupTarget = new RollupTarget(0, timeRange);
		assertEquals(Long.MAX_VALUE, getRollupScheduler().getScheduledRollupTime(rollupTarget));
		long insertTime = System.currentTimeMillis();
		getRollupScheduler().reportWrite(rollupTarget, insertTime);
		assertEquals(insertTime + RollupScheduler.WAIT_AFTER_WRITE_BEFORE_ROLLUP, getRollupScheduler().getScheduledRollupTime(rollupTarget));
	}

	@Test
	public void test_rollupTargetsReadyForRollup() {
		Range timeRange0to1 = new Range(0, 1);
		Range timeRange2to3 = new Range(2, 3);
		RollupTarget rollupTarget0to1 = new RollupTarget(0, timeRange0to1);
		RollupTarget rollupTarget2to3 = new RollupTarget(0, timeRange2to3);
		getRollupScheduler().reportWrite(rollupTarget0to1, 0);
		getRollupScheduler().reportWrite(rollupTarget2to3, 0);
		getRollupScheduler().reportWrite(rollupTarget2to3, System.currentTimeMillis());
		List<RollupTarget> readyForRollup = getRollupScheduler().rollupTargetsReadyForRollup();
		assertEquals(1, readyForRollup.size());
		assertTrue(readyForRollup.contains(rollupTarget0to1));
	}

	@Test
	public void test_scheduleReadyRollups() throws Exception {
		List<RollupTarget> rollupsRequested = new ArrayList<>();
		BlueCollectionOnDisk<TestValue> mockCollection = createMockCollection(rollupsRequested);
		RollupScheduler mockRollupScheduler = new RollupScheduler(mockCollection);
		Range timeRange = new Range(0, 1);
		RollupTarget rollupTarget = new RollupTarget(0, timeRange);
		assertEquals(Long.MAX_VALUE, mockRollupScheduler.getScheduledRollupTime(rollupTarget));
		mockRollupScheduler.reportWrite(rollupTarget, 0);
		assertEquals(0 + RollupScheduler.WAIT_AFTER_WRITE_BEFORE_ROLLUP, mockRollupScheduler.getScheduledRollupTime(rollupTarget));
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
		assertEquals(Long.MAX_VALUE, mockRollupScheduler.getScheduledRollupTime(rollupTarget));

		long now = System.currentTimeMillis();
		mockRollupScheduler.reportWrite(rollupTarget, now);
		assertEquals(now + RollupScheduler.WAIT_AFTER_WRITE_BEFORE_ROLLUP, mockRollupScheduler.getScheduledRollupTime(rollupTarget));

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
		assertEquals(Long.MAX_VALUE, mockRollupScheduler.getScheduledRollupTime(rollupTarget));
		mockRollupScheduler.reportWrite(rollupTarget, 0);
		assertEquals(0 + RollupScheduler.WAIT_AFTER_WRITE_BEFORE_ROLLUP, mockRollupScheduler.getScheduledRollupTime(rollupTarget));

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
		assertEquals(Long.MAX_VALUE, mockRollupScheduler.getScheduledRollupTime(rollupTarget));


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
