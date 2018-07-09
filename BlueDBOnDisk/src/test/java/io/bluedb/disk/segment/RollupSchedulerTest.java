package io.bluedb.disk.segment;

import static org.junit.Assert.*;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
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
		dbPath = Paths.get("testing_SegmentTest");
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
}
