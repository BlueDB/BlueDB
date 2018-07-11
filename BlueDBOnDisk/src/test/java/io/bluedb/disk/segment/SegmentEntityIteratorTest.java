package io.bluedb.disk.segment;

import static org.junit.Assert.*;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.junit.Test;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.BlueDbOnDisk;
import io.bluedb.disk.BlueDbOnDiskBuilder;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.collection.BlueCollectionImpl;
import io.bluedb.disk.file.BlueObjectInput;
import io.bluedb.disk.file.BlueObjectOutput;
import io.bluedb.disk.file.BlueWriteLock;
import io.bluedb.disk.file.LockManager;
import io.bluedb.disk.serialization.BlueEntity;
import junit.framework.TestCase;

public class SegmentEntityIteratorTest extends TestCase {

	BlueDbOnDisk db;
	BlueCollectionImpl<TestValue> collection;
	Path dbPath;
	LockManager<Path> lockManager;
	
	private static final long SEGMENT_ID = 42;

	@Override
	public void setUp() throws Exception {
		dbPath = Paths.get("testing_SegmentEntityIteratorTest");
		db = new BlueDbOnDiskBuilder().setPath(dbPath).build();
		collection = (BlueCollectionImpl<TestValue>) db.getCollection(TestValue.class, "testing");
		dbPath = db.getPath();
		lockManager = collection.getFileManager().getLockManager();
	}

	@Override
	public void tearDown() throws Exception {
		Files.walk(dbPath)
		.sorted(Comparator.reverseOrder())
		.map(Path::toFile)
		.forEach(File::delete);
	}

	@Test
	public void test_close() {
		Segment<TestValue> segment = createSegment();
		BlueKey key = createKey(1, 1);
		TestValue value = createValue("Anna");
		try {
			segment.insert(key, value);
			SegmentEntityIterator<TestValue> iterator = segment.getIterator(1, 1);
			assertNull(iterator.getCurrentPath());  // it should not have opened anything until it needs it
			iterator.hasNext();  // force it to open the next file
			assertNotNull(iterator.getCurrentPath());
			assertTrue(lockManager.isLocked(iterator.getCurrentPath()));
			iterator.close();
			assertFalse(lockManager.isLocked(iterator.getCurrentPath()));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_different_ranges() {
		Segment<TestValue> segment = createSegment();
		BlueKey key1 = createKey(1, 1);
		BlueKey key2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		try {
			segment.insert(key1, value1);
			segment.insert(key2, value2);

			SegmentEntityIterator<TestValue> iterator = segment.getIterator(0, 0);
			List<BlueEntity<TestValue>> entities = toList(iterator);
			iterator.close();
			assertEquals(0, entities.size());

			iterator = segment.getIterator(3, 4);
			entities = toList(iterator);
			iterator.close();
			assertEquals(0, entities.size());

			iterator = segment.getIterator(1, 1);
			entities = toList(iterator);
			iterator.close();
			assertEquals(1, entities.size());

			iterator = segment.getIterator(1, 2);
			entities = toList(iterator);
			iterator.close();
			assertEquals(2, entities.size());

		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_hasNext() {
		Segment<TestValue> segment = createSegment();
		BlueKey key1 = createKey(1, 1);
		BlueKey key2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		try {
			segment.insert(key1, value1);
			segment.insert(key2, value2);
			SegmentEntityIterator<TestValue> iterator = segment.getIterator(1, 2);
			assertTrue(iterator.hasNext());
			assertTrue(iterator.hasNext());  // make sure the second call doesn't break anything
			iterator.next();
			assertTrue(iterator.hasNext());
			iterator.next();
			assertFalse(iterator.hasNext());
			iterator.close();
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_next() {
		Segment<TestValue> segment = createSegment();
		BlueKey key1 = createKey(1, 1);
		BlueKey key2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		try {
			segment.insert(key1, value1);
			segment.insert(key2, value2);
			SegmentEntityIterator<TestValue> iterator = segment.getIterator(1, 2);
			assertTrue(iterator.hasNext());
			iterator.next();
			assertTrue(iterator.hasNext());
			iterator.next();
			assertFalse(iterator.hasNext());
			iterator.close();
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_next_rollup_before_reads() {
		Segment<TestValue> segment = createSegment();
		BlueKey key1 = createKey(1, 1);
		BlueKey key2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		try {
			segment.insert(key1, value1);
			segment.insert(key2, value2);
			SegmentEntityIterator<TestValue> iterator = segment.getIterator(1, 2); // it should now have two ranges to search
			List<BlueEntity<TestValue>> entities = toList(iterator);
			assertEquals(2, entities.size());
			
			iterator = segment.getIterator(1, 2); // it should now have two ranges to search
			long segmentSize = SegmentManager.getSegmentSize();
			TimeRange range = new TimeRange(0, segmentSize -1);
			segment.rollup(range);
			entities = toList(iterator);
			assertEquals(2, entities.size());

			iterator.close();
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_next_rollup_during_reads() {
		Segment<TestValue> segment = createSegment();
		BlueKey key1 = createKey(1, 1);
		BlueKey key2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		try {
			segment.insert(key1, value1);
			segment.insert(key2, value2);
			SegmentEntityIterator<TestValue> iterator = segment.getIterator(1, 2); // it should now have two files to read
			List<BlueEntity<TestValue>> entities = new ArrayList<>();
			entities.add(iterator.next()); // read one from the first file;

			// simulate a rollup from underneath us
			long segmentSize = SegmentManager.getSegmentSize();
			TimeRange range = new TimeRange(0, segmentSize -1);
			Path rolledUpPath = Paths.get(segment.getPath().toString(), range.toUnderscoreDelimitedString());
			try (BlueObjectOutput<BlueEntity<TestValue>> output = segment.getObjectOutputFor(rolledUpPath)) {
				output.write(new BlueEntity<TestValue>(key1, value1));
				output.write(new BlueEntity<TestValue>(key2, value2));
			}
			TimeRange rangeToRemove = new TimeRange(2,2);
			Path pathToRemove = Paths.get(segment.getPath().toString(), rangeToRemove.toUnderscoreDelimitedString());
			assertTrue(pathToRemove.toFile().delete());

			// add the remaining items
			while (iterator.hasNext()) {
				BlueEntity<TestValue> next = iterator.next();
				entities.add(next);
			}

			assertEquals(2, entities.size());

			iterator.close();
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_getNextStream_file_deleted() {
		Segment<TestValue> segment = createSegment();
		BlueKey key1 = createKey(1, 1);
		BlueKey key2 = createKey(2, 2);
		TestValue value1 = createValue("Anna");
		TestValue value2 = createValue("Bob");
		try {
			segment.insert(key1, value1);
			segment.insert(key2, value2);
			SegmentEntityIterator<TestValue> iterator = segment.getIterator(1, 2); // it should now have two files to read
			List<BlueEntity<TestValue>> entities = new ArrayList<>();
			entities.add(iterator.next()); // read one from the first file;

			// rip out the second file
			TimeRange rangeToRemove = new TimeRange(2,2);
			Path pathToRemove = Paths.get(segment.getPath().toString(), rangeToRemove.toUnderscoreDelimitedString());
			assertTrue(pathToRemove.toFile().delete());

			// add the remaining items
			while (iterator.hasNext()) {
				BlueEntity<TestValue> next = iterator.next();
				entities.add(next);
			}

			assertEquals(1, entities.size());

			iterator.close();
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_filesToRanges() {
		// TODO
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
		return collection.getSegmentManager().getFirstSegment(keyInSegment);
	}

	private List<TestValue> extractValues(List<BlueEntity<TestValue>> entities) {
		List<TestValue> values = new ArrayList<>();
		for (BlueEntity<TestValue> entity: entities) {
			values.add(entity.getValue());
		}
		return values;
	}

	private List<BlueEntity<TestValue>> toList(SegmentEntityIterator<TestValue> iterator) {
		List<BlueEntity<TestValue>> entities = new ArrayList<>();
		iterator.forEachRemaining(entities::add);
		return entities;
	}

	private List<TestValue> getAll(Segment<TestValue> segment) {
		List<TestValue> results = new ArrayList<>();
		try (SegmentEntityIterator<TestValue> iterator = segment.getIterator(Long.MIN_VALUE, Long.MAX_VALUE)) {
			while (iterator.hasNext()) {
				results.add(iterator.next().getValue());
			}
		}
		return results;
	}

	private void writeBytes(Path path, byte[] bytes) {
		File file = path.toFile();
		try (FileOutputStream fos = new FileOutputStream(file)) {
			fos.write(bytes);
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
	}
}
