package io.bluedb.disk.collection;

import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import io.bluedb.api.Condition;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.segment.Segment;
import io.bluedb.disk.segment.SegmentManager;
import io.bluedb.disk.segment.Range;
import io.bluedb.disk.serialization.BlueEntity;

public class BlueCollectionOnDiskTest extends BlueDbDiskTestBase {

	@Test
	public void test_query() throws Exception {
		TestValue value = new TestValue("Joe");
		insert(1, value);
		List<TestValue> values = getCollection().query().getList();
		assertEquals(1, values.size());
		assertTrue(values.contains(value));
	}

	@Test
	public void test_contains() throws Exception {
		TestValue value = new TestValue("Joe");
		insert(1, value);
		List<TestValue> values = getCollection().query().getList();
		assertEquals(1, values.size());
		assertTrue(values.contains(value));
	}

	@Test
	public void test_get() throws Exception {
		TestValue value = new TestValue("Joe");
		TestValue differentValue = new TestValue("Bob");
		BlueKey key = createTimeKey(10, value);
		BlueKey sameTimeDifferentValue = createTimeKey(10, differentValue);
		BlueKey sameValueDifferentTime = createTimeKey(20, value);
		BlueKey differentValueAndTime = createTimeKey(20, differentValue);
		insert(key, value);
		assertEquals(value, getCollection().get(key));
		assertNotEquals(value, differentValue);
		assertNotEquals(value, getCollection().get(sameTimeDifferentValue));
		assertNotEquals(value, getCollection().get(sameValueDifferentTime));
		assertNotEquals(value, getCollection().get(differentValueAndTime));
	}

	@Test
	public void test_insert() {
		TestValue value = new TestValue("Joe");
		BlueKey key = createTimeKey(10, value);
		insert(key, value);
		assertValueAtKey(key, value);
		try {
			getCollection().insert(key, value); // insert duplicate
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_update() throws Exception {
		BlueKey key = insert(10, new TestValue("Joe", 0));
		assertCupcakes(key, 0);
		getCollection().update(key, (v) -> v.addCupcake());
		assertCupcakes(key, 1);
	}

	@Test
	public void test_update_invalid() {
		TestValue value = new TestValue("Joe", 0);
		BlueKey key = insert(1, value);
		try {
			getCollection().update(key, (v) -> v.doSomethingNaughty());
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_delete() throws Exception {
		TestValue value = new TestValue("Joe");
		BlueKey key = insert(10, value);
		assertValueAtKey(key, value);
		getCollection().delete(key);
		assertValueNotAtKey(key, value);
	}

	@Test
	public void test_maxLong_maxInt() throws Exception {
		TestValue value = new TestValue("Joe");
		BlueKey key = createTimeKey(10, value);
		insert(key, value);
		assertValueAtKey(key, value);

		// test max long
		assertNull(getCollection().getMaxLongId());  // since the last insert was a String key;
		BlueKey timeKeyWithLong3 = new TimeKey(3L, 4);
		getCollection().insert(timeKeyWithLong3, value);
		assertNotNull(getCollection().getMaxLongId());
		assertEquals(3, getCollection().getMaxLongId().longValue());

		// test max integer
		assertNull(getCollection().getMaxIntegerId());
		BlueKey timeKeyWithInt5 = new TimeKey(5, 4);
		getCollection().insert(timeKeyWithInt5, value);
		assertNotNull(getCollection().getMaxIntegerId());
		assertEquals(5, getCollection().getMaxIntegerId().intValue());
	}

	@Test
	public void test_findMatches() throws Exception {
		TestValue valueJoe = new TestValue("Joe");
		TestValue valueBob = new TestValue("Bob");
		insert(1, valueJoe);
		insert(2, valueBob);
		List<BlueEntity<TestValue>> allEntities, entitiesWithJoe, entities3to5, entities2to3, entities0to1, entities0to0;

		Condition<TestValue> isJoe = (v) -> v.getName().equals("Joe");
		allEntities = getCollection().findMatches(0, 3, new ArrayList<>());
		entitiesWithJoe = getCollection().findMatches(0, 5, Arrays.asList(isJoe));
		entities3to5 = getCollection().findMatches(3, 5, new ArrayList<>());
		entities2to3 = getCollection().findMatches(2, 3, new ArrayList<>());
		entities0to1 = getCollection().findMatches(0, 1, new ArrayList<>());
		entities0to0 = getCollection().findMatches(0, 0, new ArrayList<>());

		assertEquals(2, allEntities.size());
		assertEquals(1, entitiesWithJoe.size());
		assertEquals(valueJoe, entitiesWithJoe.get(0).getValue());
		assertEquals(0, entities3to5.size());
		assertEquals(1, entities2to3.size());
		assertEquals(valueBob, entities2to3.get(0).getValue());
		assertEquals(1, entities0to1.size());
		assertEquals(valueJoe, entities0to1.get(0).getValue());
		assertEquals(0, entities0to0.size());
	}

	@Test
	public void test_executeTask() throws Exception {
		AtomicBoolean hasRun = new AtomicBoolean(false);
		Runnable task = new Runnable() {
			@Override
			public void run() {
				try {
					Thread.sleep(10);  // make sure this test waits for the task to be complete
					hasRun.set(true);
				} catch (InterruptedException e) {
					e.printStackTrace();
					fail();
				}
			}
		};

		getCollection().executeTask(task);
		assertTrue(hasRun.get());
	}

	@Test
	public void test_rollup() throws Exception {
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value3 = createValue("Chuck");
		List<TestValue> values;

		values = getCollection().query().getList();
		assertEquals(0, values.size());

		getCollection().insert(key1At1, value1);
		getCollection().insert(key3At3, value3);
		values = getCollection().query().getList();
		assertEquals(2, values.size());
		
		Segment<TestValue> segment = getCollection().getSegmentManager().getSegment(key1At1.getGroupingNumber());
		File[] segmentDirectoryContents = segment.getPath().toFile().listFiles();
		assertEquals(2, segmentDirectoryContents.length);

		long segmentSize = SegmentManager.getSegmentSize();
		Range offByOneSegmentTimeRange = new Range(0, segmentSize);
		Range entireFirstSegmentTimeRange = new Range(0, segmentSize -1);
		try {
			getCollection().rollup(offByOneSegmentTimeRange);
			fail();
		} catch (BlueDbException e) {}
		try {
			getCollection().rollup(entireFirstSegmentTimeRange);
		} catch (BlueDbException e) {
			fail();
		}

		values = getCollection().query().getList();
		assertEquals(2, values.size());
		segmentDirectoryContents = segment.getPath().toFile().listFiles();
		assertEquals(1, segmentDirectoryContents.length);
	}


	@Test
	public void test_scheduleRollup() throws Exception {
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value3 = createValue("Chuck");
		List<TestValue> values;

		getCollection().insert(key1At1, value1);
		getCollection().insert(key3At3, value3);
		values = getCollection().query().getList();
		assertEquals(2, values.size());
		
		Segment<TestValue> segment = getCollection().getSegmentManager().getSegment(key1At1.getGroupingNumber());
		File[] segmentDirectoryContents = segment.getPath().toFile().listFiles();
		assertEquals(2, segmentDirectoryContents.length);

		long segmentSize = SegmentManager.getSegmentSize();
		Range entireFirstSegmentTimeRange = new Range(0, segmentSize -1);
		getCollection().scheduleRollup(entireFirstSegmentTimeRange);
		waitForExecutorToFinish();

		values = getCollection().query().getList();
		assertEquals(2, values.size());
		segmentDirectoryContents = segment.getPath().toFile().listFiles();
		assertEquals(1, segmentDirectoryContents.length);
	}

	@Test
	public void test_updateAll_invalid() {
		TestValue value = new TestValue("Joe", 0);
		insert(1, value);
		try {
			getCollection().query().update((v) -> v.doSomethingNaughty());
			fail();
		} catch (BlueDbException e) {
		}
	}

	private void waitForExecutorToFinish() {
		Runnable doNothing = new Runnable() {@Override public void run() {}};
		Future<?> future = getCollection().executor.submit(doNothing);
		try {
			future.get();
		} catch (ExecutionException | InterruptedException e) {
			e.printStackTrace();
			fail();
		}
	}
}
