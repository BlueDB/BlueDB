package io.bluedb.disk.segment;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.BlueDbOnDisk;
import io.bluedb.disk.BlueDbOnDiskBuilder;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.collection.BlueCollectionImpl;
import junit.framework.TestCase;

public class SegmentTest extends TestCase {

	BlueDbOnDisk DB;
	BlueCollectionImpl<TestValue> COLLECTION;
	
	@Override
	protected void setUp() throws Exception {
		DB = new BlueDbOnDiskBuilder().build();
		COLLECTION = (BlueCollectionImpl<TestValue>) DB.getCollection(TestValue.class, "testing");
	}

	private static final long SEGMENT_ID = 42;

	@Test
	public void testContains() {
		Segment<TestValue> segment = createSegment();
		cleanup(segment);
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key2At1 = createKey(2, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		TestValue value3 = createValue("Chuck");
		try {
			assertFalse(segment.contains(key1At1));
			segment.put(key1At1, value1);
			assertTrue(segment.contains(key1At1));
			assertFalse(segment.contains(key2At1));
			assertFalse(segment.contains(key3At3));
			segment.put(key2At1, value2);
			assertTrue(segment.contains(key1At1));
			assertTrue(segment.contains(key2At1));
			assertFalse(segment.contains(key3At3));
			segment.put(key3At3, value3);
			assertTrue(segment.contains(key1At1));
			assertTrue(segment.contains(key2At1));
			assertTrue(segment.contains(key3At3));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		cleanup(segment);
	}

	@Test
	public void testPut() {
		Segment<TestValue> segment = createSegment();
		cleanup(segment);
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key2At1 = createKey(2, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		TestValue value3 = createValue("Chuck");
		try {
			assertFalse(segment.contains(key1At1));
			assertFalse(segment.contains(key2At1));
			assertFalse(segment.contains(key3At3));
			assertEquals(null, segment.get(key1At1));
			assertEquals(null, segment.get(key2At1));
			assertEquals(null, segment.get(key3At3));

			segment.put(key1At1, value1);
			assertTrue(segment.contains(key1At1));
			assertFalse(segment.contains(key2At1));
			assertFalse(segment.contains(key3At3));
			assertEquals(value1, segment.get(key1At1));
			assertEquals(null, segment.get(key2At1));
			assertEquals(null, segment.get(key3At3));

			segment.put(key2At1, value2);
			assertTrue(segment.contains(key1At1));
			assertTrue(segment.contains(key2At1));
			assertFalse(segment.contains(key3At3));
			assertEquals(value1, segment.get(key1At1));
			assertEquals(value2, segment.get(key2At1));
			assertEquals(null, segment.get(key3At3));

			segment.put(key3At3, value3);
			assertTrue(segment.contains(key1At1));
			assertTrue(segment.contains(key2At1));
			assertTrue(segment.contains(key3At3));
			assertEquals(value1, segment.get(key1At1));
			assertEquals(value2, segment.get(key2At1));
			assertEquals(value3, segment.get(key3At3));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		cleanup(segment);
	}

	@Test
	public void testDelete() {
		Segment<TestValue> segment = createSegment();
		cleanup(segment);
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key2At1 = createKey(2, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		TestValue value3 = createValue("Chuck");
		try {
			segment.put(key1At1, value1);
			segment.put(key2At1, value2);
			segment.put(key3At3, value3);
			assertTrue(segment.contains(key1At1));
			assertTrue(segment.contains(key2At1));
			assertTrue(segment.contains(key3At3));
			assertEquals(value1, segment.get(key1At1));
			assertEquals(value2, segment.get(key2At1));
			assertEquals(value3, segment.get(key3At3));
			segment.delete(key1At1);
			assertFalse(segment.contains(key1At1));
			assertTrue(segment.contains(key2At1));
			assertTrue(segment.contains(key3At3));
			assertEquals(null, segment.get(key1At1));
			assertEquals(value2, segment.get(key2At1));
			assertEquals(value3, segment.get(key3At3));
			segment.delete(key2At1);
			assertFalse(segment.contains(key1At1));
			assertFalse(segment.contains(key2At1));
			assertTrue(segment.contains(key3At3));
			assertEquals(null, segment.get(key1At1));
			assertEquals(null, segment.get(key2At1));
			assertEquals(value3, segment.get(key3At3));
			segment.delete(key3At3);
			assertFalse(segment.contains(key1At1));
			assertFalse(segment.contains(key2At1));
			assertFalse(segment.contains(key3At3));
			assertEquals(null, segment.get(key1At1));
			assertEquals(null, segment.get(key2At1));
			assertEquals(null, segment.get(key3At3));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		cleanup(segment);
	}

	@Test
	public void testGet() {
		Segment<TestValue> segment = createSegment();
		cleanup(segment);
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key2At1 = createKey(2, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		TestValue value3 = createValue("Chuck");
		try {
			assertEquals(null, segment.get(key1At1));
			assertEquals(null, segment.get(key2At1));
			assertEquals(null, segment.get(key3At3));

			segment.put(key3At3, value3);
			assertEquals(null, segment.get(key1At1));
			assertEquals(null, segment.get(key2At1));
			assertEquals(value3, segment.get(key3At3));

			segment.put(key2At1, value2);
			assertEquals(null, segment.get(key1At1));
			assertEquals(value2, segment.get(key2At1));
			assertEquals(value3, segment.get(key3At3));

			segment.put(key1At1, value1);
			assertEquals(value1, segment.get(key1At1));
			assertEquals(value2, segment.get(key2At1));
			assertEquals(value3, segment.get(key3At3));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		cleanup(segment);
	}

	@Test
	public void testGetRange() {
		Segment<TestValue> segment = createSegment();
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key2At1 = createKey(2, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		TestValue value3 = createValue("Chuck");
		try {
			segment.put(key1At1, value1);
			segment.put(key2At1, value2);
			segment.put(key3At3, value3);
			List<BlueEntity<TestValue>> entities0to0 = segment.getRange(0,0);
			List<TestValue> values0to0 = extractValues(entities0to0);
			assertEquals(0, values0to0.size());
			List<BlueEntity<TestValue>> entities0to1 = segment.getRange(0,1);
			List<TestValue> values0to1 = extractValues(entities0to1);
			assertEquals(2, values0to1.size());
			assertTrue(values0to1.contains(value1));
			assertTrue(values0to1.contains(value2));
			List<BlueEntity<TestValue>> entities1to3 = segment.getRange(1,3);
			List<TestValue> values1to3 = extractValues(entities1to3);
			assertEquals(3, values1to3.size());
			assertTrue(values1to3.contains(value1));
			assertTrue(values1to3.contains(value2));
			assertTrue(values1to3.contains(value3));
			List<BlueEntity<TestValue>> entities2to4 = segment.getRange(2,4);
			List<TestValue> values2to4 = extractValues(entities2to4);
			assertEquals(1, values2to4.size());
			assertTrue(values1to3.contains(value3));
			List<BlueEntity<TestValue>> entities4to5 = segment.getRange(4, 5);
			List<TestValue> values4to5 = extractValues(entities4to5);
			assertEquals(0, values4to5.size());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		cleanup(segment);
	}

	@Test
	public void testGetAll() {
		Segment<TestValue> segment = createSegment();
		BlueKey key1At1 = createKey(1, 1);
		BlueKey key2At1 = createKey(2, 1);
		BlueKey key3At3 = createKey(3, 3);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		TestValue value3 = createValue("Chuck");
		List<TestValue> values;
		try {
			values = segment.getAll();
			assertEquals(0, values.size());

			segment.put(key1At1, value1);
			values = segment.getAll();
			assertEquals(1, values.size());
			assertTrue(values.contains(value1));

			segment.put(key2At1, value2);
			values = segment.getAll();
			assertEquals(2, values.size());
			assertTrue(values.contains(value1));
			assertTrue(values.contains(value2));

			segment.put(key3At3, value3);
			values = segment.getAll();
			assertEquals(3, values.size());
			assertTrue(values.contains(value1));
			assertTrue(values.contains(value2));
			assertTrue(values.contains(value3));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		cleanup(segment);
	}

	@Test
	public void testToString() {
		Segment<TestValue> segment = createSegment();
		assertTrue(segment.toString().contains(segment.getPath().toString()));
		assertTrue(segment.toString().contains(segment.getClass().getSimpleName()));
	}

	@Test
	public void testEquals() {
		Segment<TestValue> segment1 = createSegment(1);
		Segment<TestValue> segment1copy = createSegment(1);
		Segment<TestValue> segmentMax = createSegment(Long.MAX_VALUE);
		Segment<TestValue> segmentNullPath = new Segment(null, null);
		assertEquals(segment1, segment1copy);
		assertFalse(segment1.equals(segmentMax));
		assertFalse(segment1.equals(null));
		assertFalse(segmentNullPath.equals(segment1));
		assertFalse(segment1.equals(segmentNullPath));
		assertFalse(segment1.equals("this is a String"));
	}

	private TestValue createValue(String name){
		return new TestValue(name);
	}

	private BlueKey createKey(long keyId, long time){
		return new TimeKey(keyId, time);
	}

	private Segment<TestValue> createSegment() {
		return createSegment(SEGMENT_ID);
	}
	
	private Segment<TestValue> createSegment(long segmentId) {
		BlueKey keyInSegment = new TimeKey(1, segmentId);
		return COLLECTION.getSegmentManager().getFirstSegment(keyInSegment);
	}

	private void cleanup(Segment<TestValue> segment) {
		File segmentFolder = segment.getPath().toFile();
		if (!segmentFolder.exists())
			return;
		for (File f: segmentFolder.listFiles()) {
			f.delete();
		}
		
	}

	private List<TestValue> extractValues(List<BlueEntity<TestValue>> entities) {
		List<TestValue> values = new ArrayList<>();
		for (BlueEntity<TestValue> entity: entities) {
			values.add(entity.getObject());
		}
		return values;
	}
}
