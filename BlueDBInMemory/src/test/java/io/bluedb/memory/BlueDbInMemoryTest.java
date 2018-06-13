package io.bluedb.memory;

import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.Test;
import io.bluedb.api.BlueCollection;
import io.bluedb.api.BlueDb;
import io.bluedb.api.BlueQuery;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.StringKey;
import io.bluedb.api.keys.TimeKey;
import junit.framework.TestCase;

public class BlueDbInMemoryTest extends TestCase {

	BlueDb db = new BlueDbInMemory();
	BlueCollection<TestValue> collection = db.getCollection(TestValue.class);
	
	@Test
	public void testInsert() {
		TestValue value = new TestValue("Joe");
		BlueKey key = createKey(10, value);
		insert(key, value);
		assertValueAtKey(key, value);
		cleanup();
	}

	@Test
	public void testGet() {
		TestValue value = new TestValue("Joe");
		TestValue differentValue = new TestValue("Bob");
		BlueKey key = createKey(10, value);
		BlueKey sameTimeDifferentValue = createKey(10, differentValue);
		BlueKey sameValueDifferentTime = createKey(20, value);
		BlueKey differentValueAndTime = createKey(20, differentValue);
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
		cleanup();
	}

	@Test
	public void testUpdate() {
		TestValue value = new TestValue("Joe", 0);
		BlueKey key = createKey(10, value);
		insert(key, value);

		try {
			TestValue storedValue = getCollection().get(key);
			assertEquals(0, storedValue.getCupcakes());
			assertNotEquals(1, storedValue.getCupcakes());

			getCollection().update(key, (v) -> v.addCupcake());
			TestValue updatedValue = getCollection().get(key);
			assertNotEquals(0, updatedValue.getCupcakes());
			assertEquals(1, updatedValue.getCupcakes());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		cleanup();
	}

	@Test
	public void testDelete() {
		TestValue value = new TestValue("Joe");
		BlueKey key = createKey(10, value);
		insert(key, value);
		try {
			assertEquals(value, getCollection().get(key));
			getCollection().delete(key);
			assertNotEquals(value, getCollection().get(key));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		cleanup();
	}

	@Test
	public void testWhere() {
		TestValue valueJoe = new TestValue("Joe");
		TestValue valueBob = new TestValue("Bob");
		insert(1, valueJoe);
		insert(2, valueBob);
		List<TestValue> storedValues;
		try {
			storedValues = getCollection().query().getList();
			assertEquals(2, storedValues.size());
			List<TestValue> joeOnly = getCollection().query().where((v) -> v.getName().equals("Joe")).getList();
			assertEquals(1, joeOnly.size());
			assertEquals(valueJoe, joeOnly.get(0));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		cleanup();
	}

	@Test
	public void testBeforeTimeFrame() {
		// TODO
	}

	@Test
	public void testBeforeTime() {
		TestValue valueAt1 = new TestValue("Joe");
		TestValue valueAt2 = new TestValue("Bob");
		insert(1, valueAt1);
		insert(2, valueAt2);
		try {
			List<TestValue> before3 = getCollection().query().beforeTime(3).getList();
			List<TestValue> before2 = getCollection().query().beforeTime(2).getList();
			List<TestValue> before1 = getCollection().query().beforeTime(1).getList();
			assertEquals(2, before3.size());
			assertEquals(1, before2.size());
			assertEquals(0, before1.size());
			assertTrue(before3.contains(valueAt2));
			assertTrue(before3.contains(valueAt1));
			assertTrue(before2.contains(valueAt1));
			assertFalse(before1.contains(valueAt1));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		cleanup();
	}

	@Test
	public void testBeforeOrAtTime() {
		TestValue valueAt1 = new TestValue("Joe");
		TestValue valueAt2 = new TestValue("Bob");
		insert(1, valueAt1);
		insert(2, valueAt2);
		try {
			List<TestValue> beforeOrAt3 = getCollection().query().beforeOrAtTime(3).getList();
			List<TestValue> beforeOrAt2 = getCollection().query().beforeOrAtTime(2).getList();
			List<TestValue> beforeOrAt1 = getCollection().query().beforeOrAtTime(1).getList();
			assertEquals(2, beforeOrAt3.size());
			assertEquals(2, beforeOrAt2.size());
			assertEquals(1, beforeOrAt1.size());
			assertTrue(beforeOrAt3.contains(valueAt2));
			assertTrue(beforeOrAt3.contains(valueAt1));
			assertTrue(beforeOrAt2.contains(valueAt2));
			assertTrue(beforeOrAt2.contains(valueAt1));
			assertFalse(beforeOrAt1.contains(valueAt2));
			assertTrue(beforeOrAt1.contains(valueAt1));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		cleanup();
	}

	@Test
	public void testAfterTimeFrame() {
		// TODO
	}

	@Test
	public void testAfterTime() {
		TestValue valueAt1 = new TestValue("Joe");
		TestValue valueAt2 = new TestValue("Bob");
		insert(1, valueAt1);
		insert(2, valueAt2);
		try {
			List<TestValue> after2 = getCollection().query().afterTime(2).getList();
			List<TestValue> after1 = getCollection().query().afterTime(1).getList();
			List<TestValue> after0 = getCollection().query().afterTime(0).getList();
			assertEquals(2, after0.size());
			assertEquals(1, after1.size());
			assertEquals(0, after2.size());
			assertTrue(after0.contains(valueAt2));
			assertTrue(after0.contains(valueAt1));
			assertTrue(after1.contains(valueAt2));
			assertFalse(after2.contains(valueAt2));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		cleanup();
	}

	@Test
	public void testAfterOrAtTime() {
		TestValue valueAt1 = new TestValue("Joe");
		TestValue valueAt2 = new TestValue("Bob");
		insert(1, valueAt1);
		insert(2, valueAt2);
		try {
			List<TestValue> afterOrAt2 = getCollection().query().afterOrAtTime(2).getList();
			List<TestValue> afterOrAt1 = getCollection().query().afterOrAtTime(1).getList();
			List<TestValue> afterOrAt0 = getCollection().query().afterOrAtTime(0).getList();
			assertEquals(2, afterOrAt0.size());
			assertEquals(2, afterOrAt1.size());
			assertEquals(1, afterOrAt2.size());
			assertTrue(afterOrAt0.contains(valueAt2));
			assertTrue(afterOrAt0.contains(valueAt1));
			assertTrue(afterOrAt1.contains(valueAt2));
			assertTrue(afterOrAt1.contains(valueAt1));
			assertTrue(afterOrAt2.contains(valueAt2));
			assertFalse(afterOrAt2.contains(valueAt1));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		cleanup();
	}

	@Test
	public void testBetween() {
		TestValue valueAt2 = new TestValue("Joe");
		TestValue valueAt3 = new TestValue("Bob");
		insert(2, valueAt2);
		insert(3, valueAt3);
		try {
			// various queries outside the range
			List<TestValue> after0before1 = getCollection().query().afterTime(0).beforeTime(1).getList();
			List<TestValue> after0beforeOrAt1 = getCollection().query().afterTime(0).beforeOrAtTime(1).getList();
			List<TestValue> afterOrAt0before1 = getCollection().query().afterOrAtTime(0).beforeTime(1).getList();
			List<TestValue> afterOrAt0beforeOrAt1 = getCollection().query().afterOrAtTime(0).beforeOrAtTime(1).getList();
			assertEquals(0, after0before1.size());
			assertEquals(0, after0beforeOrAt1.size());
			assertEquals(0, afterOrAt0before1.size());
			assertEquals(0, afterOrAt0beforeOrAt1.size());

			// various queries inside the range
			List<TestValue> after2before3 = getCollection().query().afterTime(2).beforeTime(3).getList();
			List<TestValue> after2beforeOrAt3 = getCollection().query().afterTime(2).beforeOrAtTime(3).getList();
			List<TestValue> afterOrAt2before3 = getCollection().query().afterOrAtTime(2).beforeTime(3).getList();
			List<TestValue> afterOrAt2beforeOrAt3 = getCollection().query().afterOrAtTime(2).beforeOrAtTime(3).getList();
			assertEquals(0, after2before3.size());
			assertEquals(1, after2beforeOrAt3.size());
			assertEquals(1, afterOrAt2before3.size());
			assertEquals(2, afterOrAt2beforeOrAt3.size());
			assertFalse(after2beforeOrAt3.contains(valueAt2));
			assertTrue(after2beforeOrAt3.contains(valueAt3));
			assertTrue(afterOrAt2before3.contains(valueAt2));
			assertFalse(afterOrAt2before3.contains(valueAt3));
			assertTrue(afterOrAt2beforeOrAt3.contains(valueAt2));
			assertTrue(afterOrAt2beforeOrAt3.contains(valueAt3));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		cleanup();
	}

	@Test
	public void testGetList() {
		TestValue valueJoe = new TestValue("Joe");
		TestValue valueBob = new TestValue("Bob");
		insert(1, valueJoe);
		insert(2, valueBob);
		List<TestValue> storedValues;
		try {
			storedValues = getCollection().query().getList();
			assertEquals(2, storedValues.size());
			List<TestValue> joeOnly = getCollection().query().where((v) -> v.getName().equals("Joe")).getList();
			assertEquals(1, joeOnly.size());
			assertEquals(valueJoe, joeOnly.get(0));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		cleanup();
	}
	
	@Test
	public void testGetIterator() {
		TestValue valueJoe = new TestValue("Joe");
		TestValue valueBob = new TestValue("Bob");
		insert(1, valueJoe);
		insert(2, valueBob);
		Iterator<TestValue> iter;
		try {
			iter = getCollection().query().getIterator();
			List<TestValue> list = new ArrayList<>();
			iter.forEachRemaining(list::add);
			assertEquals(2, list.size());
			assertTrue(list.contains(valueJoe));
			assertTrue(list.contains(valueBob));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		cleanup();
	}
	
	@Test
	public void testQueryUpdate() {
		TestValue valueJoe = new TestValue("Joe", 0);
		TestValue valueBob = new TestValue("Bob", 0);
		TestValue valueJosey = new TestValue("Josey", 0);
		TestValue valueBobby = new TestValue("Bobby", 0);
		insert(1, valueJoe);
		insert(2, valueBob);
		insert(2, valueJosey);
		insert(3, valueBobby);
		List<TestValue> storedValues, joseyOnly, everyoneButJosey;
		BlueQuery<TestValue> queryForJosey = getCollection().query().afterTime(1).where((v) -> v.getName().startsWith("Jo"));
		BlueQuery<TestValue> queryForEveryoneButJosey = getCollection().query().where((v) -> !v.getName().equals("Josey"));
		try {
			// sanity check
			storedValues = getCollection().query().getList();
			assertEquals(4, storedValues.size());
			assertTrue(storedValues.contains(valueJosey));
			int maxCupcakes = storedValues.stream().mapToInt((t) -> t.getCupcakes()).max().getAsInt();
			assertEquals(0, maxCupcakes);

			// test update with conditions
			queryForJosey.update((v) -> v.addCupcake());
			joseyOnly = queryForJosey.getList();
			assertEquals(1, joseyOnly.size());
			int joseyCupcakes = joseyOnly.get(0).getCupcakes();
			assertEquals(1, joseyCupcakes);
			int maxCupcakesExcludingJosey = queryForEveryoneButJosey.getList().stream().mapToInt((t) -> t.getCupcakes()).max().getAsInt();
			assertEquals(0, maxCupcakesExcludingJosey);

			// test update all
			getCollection().query().update((v) -> v.addCupcake());
			joseyOnly = queryForJosey.getList();
			assertEquals(1, joseyOnly.size());
			joseyCupcakes = joseyOnly.get(0).getCupcakes();
			assertEquals(2, joseyCupcakes);
			maxCupcakesExcludingJosey = queryForEveryoneButJosey.getList().stream().mapToInt((t) -> t.getCupcakes()).max().getAsInt();
			int minCupcakesExcludingJosey = queryForEveryoneButJosey.getList().stream().mapToInt((t) -> t.getCupcakes()).max().getAsInt();
			assertEquals(1, maxCupcakesExcludingJosey);
			assertEquals(1, minCupcakesExcludingJosey);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		cleanup();
	}
	
	@Test
	public void testQueryDelete() {
		TestValue valueJoe = new TestValue("Joe");
		TestValue valueBob = new TestValue("Bob");
		TestValue valueJosey = new TestValue("Josey");
		TestValue valueBobby = new TestValue("Bobby");
		insert(1, valueJoe);
		insert(2, valueBob);
		insert(2, valueJosey);
		insert(3, valueBobby);
		List<TestValue> storedValues;
		BlueQuery<TestValue> queryForJosey = getCollection().query().afterTime(1).where((v) -> v.getName().startsWith("Jo"));
		try {
			// sanity check
			storedValues = getCollection().query().getList();
			assertEquals(4, storedValues.size());
			assertTrue(storedValues.contains(valueJosey));

			// test if delete works with query conditions
			queryForJosey.delete();
			storedValues = getCollection().query().getList();
			assertEquals(3, storedValues.size());
			assertFalse(storedValues.contains(valueJosey));
			assertTrue(storedValues.contains(valueJoe));

			// test if delete works without conditions
			getCollection().query().delete();
			storedValues = getCollection().query().getList();
			assertEquals(0, storedValues.size());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		cleanup();
	}

	private void assertValueNotAtKey(BlueKey key, TestValue value) {
		try {
			assertNotEquals(value, getCollection().get(key));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	private void assertValueAtKey(BlueKey key, TestValue value) {
		TestValue differentValue = new TestValue("Bob");
		differentValue.setCupcakes(42);
		try {
			assertEquals(value, getCollection().get(key));
			assertNotEquals(value, differentValue);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	private void insert(long time, TestValue value) {
		BlueKey key = createKey(time, value);
		insert(key, value);
	}

	private void insert(BlueKey key, TestValue value) {
		try {
			getCollection().insert(key, value);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	private BlueCollection<TestValue> getCollection() {
		return collection;
	}
	
	private TimeKey createKey(long time, TestValue obj) {
		StringKey stringKey = new StringKey(obj.getName());
		return new TimeKey(stringKey, time);
	}

	private void cleanup() {
		try {
			getCollection().query().delete();
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}
}
