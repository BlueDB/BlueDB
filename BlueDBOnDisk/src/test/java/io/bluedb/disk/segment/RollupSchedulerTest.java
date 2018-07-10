package io.bluedb.disk.segment;

import static org.junit.Assert.*;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.bluedb.disk.BlueDbOnDisk;
import io.bluedb.disk.BlueDbOnDiskBuilder;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.collection.BlueCollectionImpl;

public class RollupSchedulerTest {

	BlueDbOnDisk db;
	BlueCollectionImpl<TestValue> collection;
	RollupScheduler rollupScheduler;
	Path dbPath;

	@Before
	public void setUp() throws Exception {
		dbPath = Paths.get("testing_RollupSchedulerTest");
		db = new BlueDbOnDiskBuilder().setPath(dbPath).build();
		collection = (BlueCollectionImpl<TestValue>) db.getCollection(TestValue.class, "testing");
		dbPath = db.getPath();
		rollupScheduler = new RollupScheduler(collection);
	}

	@After
	public void tearDown() throws Exception {
		Files.walk(dbPath)
		.sorted(Comparator.reverseOrder())
		.map(Path::toFile)
		.forEach(File::delete);
	}

	@Test
	public void test_reportInsert() {
		TimeRange timeRange = new TimeRange(2, 5);
		assertEquals(Long.MIN_VALUE, rollupScheduler.getLastInsertTime(timeRange));
		long insertTime = System.currentTimeMillis();
		rollupScheduler.reportInsert(timeRange, insertTime);
		assertEquals(insertTime, rollupScheduler.getLastInsertTime(timeRange));

		rollupScheduler.reportInsert(timeRange, insertTime - 1); // report earlier time
		assertEquals(insertTime, rollupScheduler.getLastInsertTime(timeRange));
	}

	@Test
	public void test_getLastInsertTime() {
		TimeRange timeRange = new TimeRange(2, 5);
		assertEquals(Long.MIN_VALUE, rollupScheduler.getLastInsertTime(timeRange));
		long insertTime = System.currentTimeMillis();
		rollupScheduler.reportInsert(timeRange, insertTime);
		assertEquals(insertTime, rollupScheduler.getLastInsertTime(timeRange));
	}

	@Test
	public void test_timeRangesReadyForRollup() {
		TimeRange timeRange0to1 = new TimeRange(0, 1);
		TimeRange timeRange2to3 = new TimeRange(2, 3);
		TimeRange timeRange4to5 = new TimeRange(4, 5);
		rollupScheduler.reportInsert(timeRange0to1, 0);
		rollupScheduler.reportInsert(timeRange2to3, 0);
		rollupScheduler.reportInsert(timeRange2to3, System.currentTimeMillis());
		List<TimeRange> readyForRollup = rollupScheduler.timeRangesReadyForRollup();
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
		RollupScheduler rollupScheduler = new RollupScheduler(mockCollection);
		TimeRange timeRange = new TimeRange(0, 1);
		assertEquals(Long.MIN_VALUE, rollupScheduler.getLastInsertTime(timeRange));
		rollupScheduler.reportInsert(timeRange, 0);
		assertEquals(0, rollupScheduler.getLastInsertTime(timeRange));
		rollupScheduler.scheduleReadyRollups();

		assertEquals(1, rollupsRequested.size());
		assertTrue(rollupsRequested.contains(timeRange));
	}

	@Test
	public void test_run() {
		List<TimeRange> rollupsRequested = new ArrayList<>();
		BlueCollectionImpl<TestValue> mockCollection = createMockCollection(rollupsRequested);
		RollupScheduler rollupScheduler = new RollupScheduler(mockCollection);
		TimeRange timeRange = new TimeRange(0, 1);
		assertEquals(Long.MIN_VALUE, rollupScheduler.getLastInsertTime(timeRange));
		rollupScheduler.reportInsert(timeRange, 0);
		assertEquals(0, rollupScheduler.getLastInsertTime(timeRange));

		Thread rollupSchedulerThread = new Thread(rollupScheduler);
		rollupSchedulerThread.start();

		for (int i = 0; i < 100; i++) {
			if(rollupsRequested.size() > 0)
				break;
			try { Thread.sleep(10);} catch (InterruptedException e) {e.printStackTrace();}
		}
		
		rollupScheduler.stop();

		assertEquals(1, rollupsRequested.size());
		assertTrue(rollupsRequested.contains(timeRange));
	}

	private BlueCollectionImpl<TestValue> createMockCollection(List<TimeRange> rollupsRequested) {
		return new BlueCollectionImpl<TestValue>(db, "test_RollupSchedulerTest", TestValue.class) {
			@Override
			public void scheduleRollup(TimeRange t) {
				rollupsRequested.add(t);
			}
		};
	}
}
