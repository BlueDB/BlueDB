package io.bluedb.disk;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.BlueDbOnDisk;
import io.bluedb.disk.BlueDbOnDiskBuilder;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.collection.BlueCollectionImpl;
import io.bluedb.disk.file.LockManager;
import io.bluedb.disk.segment.Segment;
import io.bluedb.disk.segment.SegmentEntityIterator;
import io.bluedb.disk.serialization.BlueEntity;
import junit.framework.TestCase;

public abstract class BlueDbDiskTestBase extends TestCase {

	BlueDbOnDisk db;
	BlueCollectionImpl<TestValue> collection;
	Path dbPath;
	LockManager<Path> lockManager;

	@Override
	protected void setUp() throws Exception {
		dbPath = Files.createTempDirectory(this.getClass().getSimpleName());
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

	public BlueCollectionImpl<TestValue> getCollection() {
		return collection;
	}

	public LockManager<Path> getLockManager() {
		return lockManager;
	}

	public TestValue createValue(String name){
		return new TestValue(name);
	}

	public BlueKey createKey(long keyId, long time){
		return new TimeKey(keyId, time);
	}

	public Segment<TestValue> getSegment(long groupingId) {
		BlueKey keyInSegment = new TimeKey(1, groupingId);
		return collection.getSegmentManager().getFirstSegment(keyInSegment);
	}

	public List<TestValue> extractValues(List<BlueEntity<TestValue>> entities) {
		List<TestValue> values = new ArrayList<>();
		for (BlueEntity<TestValue> entity: entities) {
			values.add(entity.getValue());
		}
		return values;
	}
	
	public List<TestValue> getAll(Segment<TestValue> segment) {
		List<TestValue> results = new ArrayList<>();
		try (SegmentEntityIterator<TestValue> iterator = segment.getIterator(Long.MIN_VALUE, Long.MAX_VALUE)) {
			while (iterator.hasNext()) {
				results.add(iterator.next().getValue());
			}
		}
		return results;
	}

	public void writeBytes(Path path, byte[] bytes) {
		File file = path.toFile();
		try (FileOutputStream fos = new FileOutputStream(file)) {
			fos.write(bytes);
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
	}

	public static <X> List<X> toList(Iterator<X> iterator) {
		List<X> results = new ArrayList<>();
		while (iterator.hasNext()) {
			X next = iterator.next();
			results.add(next);
		}
		return results;
	}
}
