package org.bluedb.disk.segment.rollup;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.mockito.Mockito;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.BlueCollectionOnDisk;
import org.bluedb.disk.collection.CollectionTestTools;
import org.bluedb.disk.collection.index.BlueIndexOnDisk;
import org.bluedb.disk.collection.index.TestRetrievalKeyExtractor;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.Segment;
import org.bluedb.disk.segment.rollup.RollupScheduler;

public class RollupSchedulerTest extends BlueDbDiskTestBase {

	@Test
	public void test_reportWrite() {
		Range timeRange = new Range(2, 5);
		Range timeRangeCopy = new Range(2, 5);
		RollupTarget rollupTarget = new RollupTarget(0, timeRange);
		RollupTarget rollupTargetCopy = new RollupTarget(0, timeRangeCopy);
		assertEquals(Long.MAX_VALUE, getRollupScheduler().getScheduledRollupTime(rollupTarget));
		assertEquals(0, getRollupScheduler().getRollupTimes().size());
		long insertTime = System.currentTimeMillis();
		getRollupScheduler().reportWrite(rollupTarget, insertTime);
		assertEquals(insertTime + rollupTarget.getWriteRollupDelay(), getRollupScheduler().getScheduledRollupTime(rollupTarget));
		assertEquals(1, getRollupScheduler().getRollupTimes().size());

		getRollupScheduler().reportWrite(rollupTarget, insertTime - 1); // report earlier time
		assertEquals(insertTime + rollupTarget.getWriteRollupDelay(), getRollupScheduler().getScheduledRollupTime(rollupTarget));
		assertEquals(1, getRollupScheduler().getRollupTimes().size());

		// avoid duplicate scheduling
		getRollupScheduler().reportWrite(rollupTargetCopy, insertTime);
		assertEquals(1, getRollupScheduler().getRollupTimes().size());
	}

	@Test
	public void test_reportRead() {
		Range timeRange = new Range(2, 5);
		Range timeRangeCopy = new Range(2, 5);
		RollupTarget rollupTarget = new RollupTarget(0, timeRange);
		RollupTarget rollupTargetCopy = new RollupTarget(0, timeRangeCopy);
		assertEquals(0, getRollupScheduler().getRollupTimes().size());
		assertEquals(Long.MAX_VALUE, getRollupScheduler().getScheduledRollupTime(rollupTarget));
		long readTime = System.currentTimeMillis();
		getRollupScheduler().reportRead(rollupTarget, readTime);
		assertEquals(readTime + rollupTarget.getReadRollupDelay(), getRollupScheduler().getScheduledRollupTime(rollupTarget));
		assertEquals(1, getRollupScheduler().getRollupTimes().size());

		getRollupScheduler().reportRead(rollupTarget, readTime - 1); // report earlier time
		assertEquals(readTime + rollupTarget.getReadRollupDelay(), getRollupScheduler().getScheduledRollupTime(rollupTarget));
		assertEquals(1, getRollupScheduler().getRollupTimes().size());

		// avoid duplicate scheduling
		getRollupScheduler().reportRead(rollupTarget, readTime);
        assertEquals(readTime + rollupTarget.getReadRollupDelay(), getRollupScheduler().getScheduledRollupTime(rollupTarget));
		assertEquals(1, getRollupScheduler().getRollupTimes().size());
	}

	@Test
	public void test_getLastWriteTime() {
		Range timeRange = new Range(2, 5);
		RollupTarget rollupTarget = new RollupTarget(0, timeRange);
		assertEquals(Long.MAX_VALUE, getRollupScheduler().getScheduledRollupTime(rollupTarget));
		long insertTime = System.currentTimeMillis();
		getRollupScheduler().reportWrite(rollupTarget, insertTime);
		assertEquals(insertTime + rollupTarget.getWriteRollupDelay(), getRollupScheduler().getScheduledRollupTime(rollupTarget));
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
		assertEquals(0, mockRollupScheduler.rollupTargetsReadyForRollup().size());
		mockRollupScheduler.reportWrite(rollupTarget, 0);
		assertEquals(1, mockRollupScheduler.rollupTargetsReadyForRollup().size());
		assertEquals(0 + rollupTarget.getWriteRollupDelay(), mockRollupScheduler.getScheduledRollupTime(rollupTarget));
		mockRollupScheduler.scheduleReadyRollups(Integer.MAX_VALUE);
		assertEquals(0, mockRollupScheduler.rollupTargetsReadyForRollup().size());

		assertEquals(1, rollupsRequested.size());
		assertTrue(rollupsRequested.contains(rollupTarget));
	}

	@Test
	public void test_reportRead_limit() {
		BlueCollectionOnDisk<?> busyCollection = Mockito.mock(BlueCollectionOnDisk.class);
		RollupScheduler rollupScheduler = new RollupScheduler(busyCollection);
		Mockito.doReturn(30).when(busyCollection).getQueuedTaskCount();
		for (int i=0; i<30; i++) {
			rollupScheduler.reportRead(new RollupTarget(i, new Range(i, i)), 0);
		}
		rollupScheduler.scheduleLimitedReadyRollups();

		BlueCollectionOnDisk<?> nonBusyCollection = Mockito.mock(BlueCollectionOnDisk.class);
		Mockito.doReturn(0).when(nonBusyCollection).getQueuedTaskCount();
		RollupScheduler nonBusyRollupScheduler = new RollupScheduler(nonBusyCollection);
		for (int i=0; i<30; i++) {
			nonBusyRollupScheduler.reportRead(new RollupTarget(i, new Range(i, i)), 0);
		}
		nonBusyRollupScheduler.scheduleLimitedReadyRollups();
		Mockito.verify(busyCollection, Mockito.atMost(0)).submitTask(Mockito.any());
		Mockito.verify(nonBusyCollection, Mockito.atLeast(20)).submitTask(Mockito.any());
	}

	@Test
	public void test_scheduleRollup_collection() throws Exception {
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value3 = createValue("Chuck");
		List<TestValue> values;

		getTimeCollection().insert(key1At1, value1);
		getTimeCollection().insert(key3At3, value3);
		values = getTimeCollection().query().getList();
		assertEquals(2, values.size());

		Segment<TestValue> segment = getTimeCollection().getSegmentManager().getSegment(key1At1.getGroupingNumber());
		File[] segmentDirectoryContents = segment.getPath().toFile().listFiles();
		assertEquals(2, segmentDirectoryContents.length);

		long segmentSize = getTimeCollection().getSegmentManager().getSegmentSize();
		Range entireFirstSegmentTimeRange = new Range(0, segmentSize -1);
		RollupTarget rollupTarget = new RollupTarget(0, entireFirstSegmentTimeRange);
		getTimeCollection().getRollupScheduler().scheduleRollup(rollupTarget);
		CollectionTestTools.waitForExecutorToFinish(getTimeCollection());

		values = getTimeCollection().query().getList();
		assertEquals(2, values.size());
		segmentDirectoryContents = segment.getPath().toFile().listFiles();
		assertEquals(1, segmentDirectoryContents.length);
	}

	@Test
	public void test_scheduleRollup_index() throws Exception {
		TestRetrievalKeyExtractor keyExtractor = new TestRetrievalKeyExtractor();
		BlueCollectionOnDisk<TestValue> collection = getTimeCollection();
		String indexName = "test_index";
		BlueIndex<IntegerKey, TestValue> index = collection.createIndex(indexName, IntegerKey.class, keyExtractor);
		BlueIndexOnDisk<IntegerKey, TestValue> indexOnDisk = (BlueIndexOnDisk<IntegerKey, TestValue>) index;

		BlueKey key1At1 = createKey(1, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna", 1);
		TestValue value3 = createValue("Chuck", 3);
		List<TestValue> values;

		values = collection.query().getList();
		assertEquals(0, values.size());

		collection.insert(key1At1, value1);
		collection.insert(key3At3, value3);
		values = collection.query().getList();
		assertEquals(2, values.size());

		BlueKey retrievalKey1 = keyExtractor.extractKeys(value1).get(0);
		Segment<?> indexSegment = indexOnDisk.getSegmentManager().getSegment(retrievalKey1.getGroupingNumber());
		File segmentFolder = indexSegment.getPath().toFile();
		File[] segmentDirectoryContents = segmentFolder.listFiles();
		assertEquals(2, segmentDirectoryContents.length);

		
		Range entireFirstSegmentRange = indexSegment.getRange();
		IndexRollupTarget rollupTarget = new IndexRollupTarget(indexName, 0, entireFirstSegmentRange);
		collection.getRollupScheduler().scheduleRollup(rollupTarget);
		collection.getRollupScheduler().scheduleRollup(rollupTarget);
		CollectionTestTools.waitForExecutorToFinish(collection);

		values = collection.query().getList();
		assertEquals(2, values.size());
		segmentDirectoryContents = segmentFolder.listFiles();
		assertEquals(1, segmentDirectoryContents.length);
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
		assertEquals(now + rollupTarget.getWriteRollupDelay(), mockRollupScheduler.getScheduledRollupTime(rollupTarget));

		mockRollupScheduler.scheduleReadyRollups(Integer.MAX_VALUE);
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
		assertEquals(0 + rollupTarget.getWriteRollupDelay(), mockRollupScheduler.getScheduledRollupTime(rollupTarget));

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
            public void submitTask(Runnable r) {
            	RollupTask rollupTask = (RollupTask) r;
                rollupsRequested.add(rollupTask.getTarget());
            }
        };
	}
}
