package io.bluedb.disk.collection;

import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import io.bluedb.api.BlueDb;
import io.bluedb.api.Condition;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.StringKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.BlueDbOnDiskBuilder;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.segment.Segment;
import io.bluedb.disk.segment.SegmentManager;
import io.bluedb.disk.segment.TimeRange;
import io.bluedb.disk.serialization.BlueEntity;
import junit.framework.TestCase;

public class BlueCollectionImplTest extends BlueDbDiskTestBase {

	@Test
	public void test_query() {
		TestValue value = new TestValue("Joe");
		insert(1, value);
		try {
			List<TestValue> values = getCollection().query().getList();
			assertEquals(1, values.size());
			assertTrue(values.contains(value));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_contains() {
		TestValue value = new TestValue("Joe");
		insert(1, value);
		try {
			List<TestValue> values = getCollection().query().getList();
			assertEquals(1, values.size());
			assertTrue(values.contains(value));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_get() {
		TestValue value = new TestValue("Joe");
		TestValue differentValue = new TestValue("Bob");
		BlueKey key = createTimeKey(10, value);
		BlueKey sameTimeDifferentValue = createTimeKey(10, differentValue);
		BlueKey sameValueDifferentTime = createTimeKey(20, value);
		BlueKey differentValueAndTime = createTimeKey(20, differentValue);
		insert(key, value);
		try {
			assertEquals(value, getCollection().get(key));
			assertNotEquals(value, differentValue);
			assertNotEquals(value, getCollection().get(sameTimeDifferentValue));
			assertNotEquals(value, getCollection().get(sameValueDifferentTime));
			assertNotEquals(value, getCollection().get(differentValueAndTime));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
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
	public void test_update() {
		BlueKey key = insert(10, new TestValue("Joe", 0));
		try {
			assertCupcakes(key, 0);
			getCollection().update(key, (v) -> v.addCupcake());
			assertCupcakes(key, 1);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
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
	public void test_delete() {
		TestValue value = new TestValue("Joe");
		BlueKey key = insert(10, value);
		try {
			assertValueAtKey(key, value);
			getCollection().delete(key);
			assertValueNotAtKey(key, value);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_findMatches() {
		TestValue valueJoe = new TestValue("Joe");
		TestValue valueBob = new TestValue("Bob");
		insert(1, valueJoe);
		insert(2, valueBob);
		List<BlueEntity<TestValue>> allEntities, entitiesWithJoe, entities3to5, entities2to3, entities0to1, entities0to0;
		try {
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

		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_executeTask() {
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
		try {
			getCollection().executeTask(task);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		assertTrue(hasRun.get());
	}

	@Test
	public void test_rollup() {
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value3 = createValue("Chuck");
		List<TestValue> values;
		try {
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
			TimeRange offByOneSegmentTimeRange = new TimeRange(0, segmentSize);
			TimeRange entireFirstSegmentTimeRange = new TimeRange(0, segmentSize -1);
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

		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}


	@Test
	public void test_scheduleRollup() {
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value3 = createValue("Chuck");
		List<TestValue> values;
		try {
			getCollection().insert(key1At1, value1);
			getCollection().insert(key3At3, value3);
			values = getCollection().query().getList();
			assertEquals(2, values.size());
			
			Segment<TestValue> segment = getCollection().getSegmentManager().getSegment(key1At1.getGroupingNumber());
			File[] segmentDirectoryContents = segment.getPath().toFile().listFiles();
			assertEquals(2, segmentDirectoryContents.length);

			long segmentSize = SegmentManager.getSegmentSize();
			TimeRange entireFirstSegmentTimeRange = new TimeRange(0, segmentSize -1);
			getCollection().scheduleRollup(entireFirstSegmentTimeRange);
			waitForExecutorToFinish();

			values = getCollection().query().getList();
			assertEquals(2, values.size());
			segmentDirectoryContents = segment.getPath().toFile().listFiles();
			assertEquals(1, segmentDirectoryContents.length);

		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
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
