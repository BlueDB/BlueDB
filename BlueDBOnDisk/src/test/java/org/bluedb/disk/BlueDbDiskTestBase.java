package org.bluedb.disk;

import static org.junit.Assert.assertNotEquals;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.HashGroupedKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.api.keys.UUIDKey;
import org.bluedb.disk.BlueDbOnDisk;
import org.bluedb.disk.BlueDbOnDiskBuilder;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.BlueCollectionOnDisk;
import org.bluedb.disk.collection.CollectionMetaData;
import org.bluedb.disk.file.FileManager;
import org.bluedb.disk.lock.LockManager;
import org.bluedb.disk.recovery.RecoveryManager;
import org.bluedb.disk.segment.Segment;
import org.bluedb.disk.segment.SegmentEntityIterator;
import org.bluedb.disk.segment.SegmentManager;
import org.bluedb.disk.segment.rollup.RollupScheduler;
import org.bluedb.disk.serialization.BlueEntity;
import org.bluedb.disk.serialization.BlueSerializer;
import junit.framework.TestCase;

public abstract class BlueDbDiskTestBase extends TestCase {

	private static String TIME_COLLECTION_NAME = "testing_time";
	private static String HASH_GROUPED_COLLECTION_NAME = "testing_value";
	private static String LONG_COLLECTION_NAME = "long_value";
	private static String INT_COLLECTION_NAME = "int_value";
	BlueDbOnDisk db;
	BlueCollectionOnDisk<TestValue> timeCollection;
	BlueCollectionOnDisk<TestValue> hashGroupedCollection;
	BlueCollectionOnDisk<TestValue> intCollection;
	BlueCollectionOnDisk<TestValue> longCollection;
	Path dbPath;
	LockManager<Path> lockManager;
	RollupScheduler rollupScheduler;
	CollectionMetaData metaData;
	List<File> filesToDelete;
	CollectionMetaData timeCollectionMetaData;

	@Override
	protected void setUp() throws Exception {
		filesToDelete = new ArrayList<>();
		dbPath = createTempFolder().toPath();
		db = new BlueDbOnDiskBuilder().setPath(dbPath).build();
		timeCollection = db.collectionBuilder(TimeKey.class, TestValue.class).withName(TIME_COLLECTION_NAME).build();
		hashGroupedCollection = db.collectionBuilder(HashGroupedKey.class, TestValue.class).withName(HASH_GROUPED_COLLECTION_NAME).build();
		longCollection = db.collectionBuilder(LongKey.class, TestValue.class).withName(LONG_COLLECTION_NAME).build();
		intCollection = db.collectionBuilder(IntegerKey.class, TestValue.class).withName(INT_COLLECTION_NAME).build();
		dbPath = db.getPath();
		lockManager = timeCollection.getFileManager().getLockManager();
		rollupScheduler = new RollupScheduler(timeCollection);
		timeCollectionMetaData = getTimeCollection().getMetaData();
	}

	@Override
	public void tearDown() throws Exception {
		Files.walk(dbPath)
		.sorted(Comparator.reverseOrder())
		.map(Path::toFile)
		.forEach(File::delete);
		for (File file: filesToDelete) {
			Blutils.recursiveDelete(file);
		}
	}

	public BlueCollectionOnDisk<TestValue> getTimeCollection() {
		return timeCollection;
	}

	public SegmentManager<TestValue> getTimeSegmentManager() {
		return timeCollection.getSegmentManager();
	}

	public BlueCollectionOnDisk<TestValue> getHashGroupedCollection() {
		return hashGroupedCollection;
	}

	public BlueCollectionOnDisk<TestValue> getLongCollection() {
		return longCollection;
	}
	
	public BlueCollectionOnDisk<TestValue> getIntCollection() {
		return intCollection;
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
		return timeCollection.getSegmentManager().getFirstSegment(keyInSegment);
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

	public BlueKey insertAtTime(long time, TestValue value) {
		BlueKey key = createTimeKey(time, value);
		insertToTimeCollection(key, value);
		return key;
	}

	public IntegerKey insertAtInteger(int id, TestValue value) {
		IntegerKey key = new IntegerKey(id);
		insertToIntCollection(key, value);
		return key;
	}

	public HashGroupedKey insertAtId(UUID id, TestValue value) {
		HashGroupedKey key = new UUIDKey(id);
		insertToHashGroupedCollection(key, value);
		return key;
	}

	public LongKey insertAtLong(long id, TestValue value) {
		LongKey key = new LongKey(id);
		insertToLongCollection(key, value);
		return key;
	}

	public void insertToIntCollection(IntegerKey key, TestValue value) {
		try {
			getIntCollection().insert(key, value);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	public void insertToHashGroupedCollection(HashGroupedKey key, TestValue value) {
		try {
			getHashGroupedCollection().insert(key, value);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	public void insertToLongCollection(LongKey key, TestValue value) {
		try {
			getLongCollection().insert(key, value);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	public void insertToTimeCollection(BlueKey key, TestValue value) {
		try {
			getTimeCollection().insert(key, value);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	public BlueKey insertAtTimeFrame(long start, long end, TestValue value) {
		BlueKey key = createTimeFrameKey(start, end, value);
		insertToTimeCollection(key, value);
		return key;
	}

	public TimeKey createTimeKey(long time, TestValue obj) {
		StringKey stringKey = new StringKey(obj.getName());
		return new TimeKey(stringKey, time);
	}

	public void assertCupcakes(BlueKey key, int cupcakes) {
		try {
			TestValue value = getTimeCollection().get(key);
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
			assertNotEquals(value, getTimeCollection().get(key));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	public void assertValueAtKey(BlueKey key, TestValue value) {
		TestValue differentValue = new TestValue("Bob");
		differentValue.setCupcakes(42);
		try {
			assertEquals(value, getTimeCollection().get(key));
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
		return timeCollection.getFileManager();
	}

	public RollupScheduler getRollupScheduler() {
		return rollupScheduler;
	}

	public BlueSerializer getSerializer() {
		return getTimeCollection().getSerializer();
	}

	public CollectionMetaData getTimeCollectionMetaData() {
		return timeCollectionMetaData;
	}

	public void removeKey(BlueKey key) {
		try {
			getTimeCollection().delete(key);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	public RecoveryManager<TestValue> getRecoveryManager() {
		return timeCollection.getRecoveryManager();
	}

	public TestValue createValue(String name, int cupcakes){
		return new TestValue(name, cupcakes);
	}

    public String getTimeCollectionName() {
        return TIME_COLLECTION_NAME;
    }

	public File createTempFolder() {
		return createTempFolder(getClass().getSimpleName());
	}

	public File createTempFolder(String tempFolderName) {
		try {
			Path tempFolderPath = Files.createTempDirectory(tempFolderName);
			File tempFolder = tempFolderPath.toFile();
			tempFolder.deleteOnExit();
			filesToDelete.add(tempFolder);
			return tempFolder;
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		return null;
	}

	public static <T extends Serializable> List<T> toValueList(Iterator<BlueEntity<T>> iterator) {
		List<T> results = new ArrayList<>();
		while(iterator.hasNext()) {
			results.add(iterator.next().getValue());
		}
		return results;
	}

	public static <T extends Serializable> List<BlueEntity<T>> toEntityList(Iterator<BlueEntity<T>> iterator) {
		List<BlueEntity<T>> results = new ArrayList<>();
		while(iterator.hasNext()) {
			results.add(iterator.next());
		}
		return results;
	}
}
