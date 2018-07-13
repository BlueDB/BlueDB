package io.bluedb.disk;

import static org.junit.Assert.assertNotEquals;
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
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.StringKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.BlueDbOnDisk;
import io.bluedb.disk.BlueDbOnDiskBuilder;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.collection.CollectionMetaData;
import io.bluedb.disk.file.FileManager;
import io.bluedb.disk.lock.LockManager;
import io.bluedb.disk.recovery.RecoveryManager;
import io.bluedb.disk.segment.Segment;
import io.bluedb.disk.segment.SegmentEntityIterator;
import io.bluedb.disk.segment.rollup.RollupScheduler;
import io.bluedb.disk.serialization.BlueEntity;
import io.bluedb.disk.serialization.BlueSerializer;
import junit.framework.TestCase;

public abstract class BlueDbDiskTestBase extends TestCase {

	BlueDbOnDisk db;
	BlueCollectionOnDisk<TestValue> collection;
	Path dbPath;
	LockManager<Path> lockManager;
	RollupScheduler rollupScheduler;
	CollectionMetaData metaData;

	@Override
	protected void setUp() throws Exception {
		dbPath = Files.createTempDirectory(this.getClass().getSimpleName());
		db = new BlueDbOnDiskBuilder().setPath(dbPath).build();
		collection = (BlueCollectionOnDisk<TestValue>) db.getCollection(TestValue.class, "testing");
		dbPath = db.getPath();
		lockManager = collection.getFileManager().getLockManager();
		rollupScheduler = new RollupScheduler(collection);
		metaData = getCollection().getMetaData();
	}

	@Override
	public void tearDown() throws Exception {
		Files.walk(dbPath)
		.sorted(Comparator.reverseOrder())
		.map(Path::toFile)
		.forEach(File::delete);
	}

	public BlueCollectionOnDisk<TestValue> getCollection() {
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

	public Segment<TestValue> getSegment() {
		return getSegment(42);
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

	public TimeKey createTimeFrameKey(long start, long end, TestValue obj) {
		StringKey stringKey = new StringKey(obj.getName());
		return new TimeFrameKey(stringKey, start, end);
	}

	public BlueKey insert(long time, TestValue value) {
		BlueKey key = createTimeKey(time, value);
		insert(key, value);
		return key;
	}

	public void insert(BlueKey key, TestValue value) {
		try {
			getCollection().insert(key, value);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	public BlueKey insert(long start, long end, TestValue value) {
		BlueKey key = createTimeFrameKey(start, end, value);
		insert(key, value);
		return key;
	}

	public TimeKey createTimeKey(long time, TestValue obj) {
		StringKey stringKey = new StringKey(obj.getName());
		return new TimeKey(stringKey, time);
	}

	public void assertCupcakes(BlueKey key, int cupcakes) {
		try {
			TestValue value = getCollection().get(key);
			if (value == null)
				fail();
			assertEquals(cupcakes, value.getCupcakes());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	public void assertValueNotAtKey(BlueKey key, TestValue value) {
		try {
			assertNotEquals(value, getCollection().get(key));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	public void assertValueAtKey(BlueKey key, TestValue value) {
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

	public Path getPath() {
		return dbPath;
	}

	public File createJunkFolder(File parentFolder, String folderName) {
		File file = Paths.get(parentFolder.toPath().toString(), folderName).toFile();
		file.mkdir();
		return file;
	}

	public File createJunkFile(File parentFolder, String fileName) {
		File file = Paths.get(parentFolder.toPath().toString(), fileName).toFile();
		try {
			file.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return file;
	}

	public File createSubfolder(File parentFolder, long value) {
		String subfolderName = String.valueOf(value);
		File file = Paths.get(parentFolder.toPath().toString(), subfolderName).toFile();
		file.mkdir();
		return file;
	}

	public void emptyAndDelete(File folder) {
		if (folder.exists()) {
			for (File f: folder.listFiles()) {
				if (f.isDirectory()) {
					emptyAndDelete(f);
				} else {
					f.delete();
				}
			}
			folder.delete();
		}
	}

	public BlueDbOnDisk db() {
		return db;
	}

	public FileManager getFileManager() {
		return collection.getFileManager();
	}

	public RollupScheduler getRollupScheduler() {
		return rollupScheduler;
	}

	public BlueSerializer getSerializer() {
		return getCollection().getSerializer();
	}

	public CollectionMetaData getMetaData() {
		return metaData;
	}

	public void removeKey(BlueKey key) {
		try {
			getCollection().delete(key);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	public RecoveryManager<TestValue> getRecoveryManager() {
		return collection.getRecoveryManager();
	}

	public TestValue createValue(String name, int cupcakes){
		return new TestValue(name, cupcakes);
	}
}
