package org.bluedb.disk.helpers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.io.Closeable;
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
import java.util.Objects;
import java.util.UUID;

import org.bluedb.api.BlueCollectionVersion;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.HashGroupedKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.api.keys.UUIDKey;
import org.bluedb.disk.BlueDbOnDiskBuilder;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.ReadWriteDbOnDisk;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.collection.ReadWriteTimeCollectionOnDisk;
import org.bluedb.disk.collection.metadata.ReadWriteCollectionMetaData;
import org.bluedb.disk.helpers.config.TestDefaultConfigurationService;
import org.bluedb.disk.lock.LockManager;
import org.bluedb.disk.models.calls.Call;
import org.bluedb.disk.segment.ReadWriteSegment;
import org.bluedb.disk.segment.SegmentEntityIterator;
import org.bluedb.disk.segment.rollup.RollupScheduler;
import org.bluedb.disk.serialization.BlueEntity;

public class BlueDbOnDiskWrapper implements Closeable {

	private static final String TIME_COLLECTION_NAME = "testing_time";
	private static final String HASH_GROUPED_COLLECTION_NAME = "testing_value";
	private static final String LONG_COLLECTION_NAME = "long_value";
	private static final String INT_COLLECTION_NAME = "int_value";
	private static final String CALL_COLLECTION_NAME = "call_collection";

	private final List<File> filesToDelete = new ArrayList<>();
	private final ReadWriteDbOnDisk db;
	private final Path dbPath;
	private final ReadWriteTimeCollectionOnDisk<TestValue> timeCollection;
	private final ReadWriteCollectionOnDisk<TestValue> hashGroupedCollection;
	private final ReadWriteCollectionOnDisk<TestValue> intCollection;
	private final ReadWriteCollectionOnDisk<TestValue> longCollection;
	private final ReadWriteCollectionOnDisk<Call> callCollection;
	private final LockManager<Path> lockManager;
	private final RollupScheduler rollupScheduler;
	private final ReadWriteCollectionMetaData timeCollectionMetaData;
	private final SimpleEncryptionService encryptionService;

	public BlueDbOnDiskWrapper(StartupOption startupOption, BlueCollectionVersion requestedCollectionVersion) throws BlueDbException {
		dbPath = createTempFolder().toPath();
		encryptionService = new SimpleEncryptionService();

		switch (startupOption) {
			case EncryptionEnabled:
				db = (ReadWriteDbOnDisk) new BlueDbOnDiskBuilder()
					.withPath(dbPath)
					.withConfigurationService(new TestDefaultConfigurationService())
					.withEncryptionService(encryptionService)
					.build();
				break;
			case EncryptionDisabled:
				encryptionService.setEncryptionEnabled(false);
				db = (ReadWriteDbOnDisk) new BlueDbOnDiskBuilder()
						.withPath(dbPath)
						.withConfigurationService(new TestDefaultConfigurationService())
						.withEncryptionService(encryptionService)
						.build();
				break;
			default:
				throw new IllegalStateException("Unexpected value: " + startupOption);
		}
		timeCollection = (ReadWriteTimeCollectionOnDisk<TestValue>) db.getTimeCollectionBuilder(TIME_COLLECTION_NAME, TimeKey.class, TestValue.class).withCollectionVersion(requestedCollectionVersion).build();
		hashGroupedCollection = (ReadWriteCollectionOnDisk<TestValue>) db.getCollectionBuilder(HASH_GROUPED_COLLECTION_NAME, HashGroupedKey.class, TestValue.class).withCollectionVersion(requestedCollectionVersion).build();
		longCollection = (ReadWriteCollectionOnDisk<TestValue>) db.getCollectionBuilder(LONG_COLLECTION_NAME, LongKey.class, TestValue.class).withCollectionVersion(requestedCollectionVersion).build();
		callCollection = (ReadWriteCollectionOnDisk<Call>) db.getCollectionBuilder(CALL_COLLECTION_NAME, TimeFrameKey.class, Call.class).withOptimizedClasses(Call.getClassesToRegisterAsList()).withCollectionVersion(requestedCollectionVersion).build();
		intCollection = (ReadWriteCollectionOnDisk<TestValue>) db.getCollectionBuilder(INT_COLLECTION_NAME, IntegerKey.class, TestValue.class).withCollectionVersion(requestedCollectionVersion).build();
		lockManager = timeCollection.getFileManager().getLockManager();
		rollupScheduler = new RollupScheduler(timeCollection);
		timeCollectionMetaData = getTimeCollection().getMetaData();
	}

	public BlueDbOnDiskWrapper(StartupOption startupOption, Path path, BlueCollectionVersion requestedCollectionVersion) throws BlueDbException {
		dbPath = path;
		encryptionService = new SimpleEncryptionService();

		switch (startupOption) {
			case EncryptionEnabled:
				db = (ReadWriteDbOnDisk) new BlueDbOnDiskBuilder()
					.withPath(dbPath)
					.withConfigurationService(new TestDefaultConfigurationService())
					.withEncryptionService(encryptionService)
					.build();
				break;
			case EncryptionDisabled:
				encryptionService.setEncryptionEnabled(false);
				db = (ReadWriteDbOnDisk) new BlueDbOnDiskBuilder()
						.withPath(dbPath)
						.withConfigurationService(new TestDefaultConfigurationService())
						.withEncryptionService(encryptionService)
						.build();
				break;
			default:
				throw new IllegalStateException("Unexpected value: " + startupOption);
		}
		timeCollection = (ReadWriteTimeCollectionOnDisk<TestValue>) db.getTimeCollectionBuilder(TIME_COLLECTION_NAME, TimeKey.class, TestValue.class).withCollectionVersion(requestedCollectionVersion).build();
		hashGroupedCollection = (ReadWriteCollectionOnDisk<TestValue>) db.getCollectionBuilder(HASH_GROUPED_COLLECTION_NAME, HashGroupedKey.class, TestValue.class).withCollectionVersion(requestedCollectionVersion).build();
		longCollection = (ReadWriteCollectionOnDisk<TestValue>) db.getCollectionBuilder(LONG_COLLECTION_NAME, LongKey.class, TestValue.class).withCollectionVersion(requestedCollectionVersion).build();
		callCollection = (ReadWriteCollectionOnDisk<Call>) db.getCollectionBuilder(CALL_COLLECTION_NAME, TimeFrameKey.class, Call.class).withOptimizedClasses(Call.getClassesToRegisterAsList()).withCollectionVersion(requestedCollectionVersion).build();
		intCollection = (ReadWriteCollectionOnDisk<TestValue>) db.getCollectionBuilder(INT_COLLECTION_NAME, IntegerKey.class, TestValue.class).withCollectionVersion(requestedCollectionVersion).build();
		lockManager = timeCollection.getFileManager().getLockManager();
		rollupScheduler = new RollupScheduler(timeCollection);
		timeCollectionMetaData = getTimeCollection().getMetaData();
	}

	@Override
	public void close() throws IOException {
		Files.walk(dbPath)
				.sorted(Comparator.reverseOrder())
				.map(Path::toFile)
				.forEach(File::delete);
		for (File file : filesToDelete) {
			Blutils.recursiveDelete(file);
		}
	}
	
	public ReadWriteSegment<TestValue> getSegment(long groupingId) {
		BlueKey keyInSegment = new TimeKey(1, groupingId);
		return timeCollection.getSegmentManager().getFirstSegment(keyInSegment);
	}

	public List<TestValue> extractValues(List<BlueEntity<TestValue>> entities) {
		List<TestValue> values = new ArrayList<>();
		for (BlueEntity<TestValue> entity : entities) {
			values.add(entity.getValue());
		}
		return values;
	}

	public int countItems(ReadWriteSegment<TestValue> segment) {
		return getAll(segment).size();
	}

	public int countFiles(ReadWriteSegment<TestValue> segment) {
		File[] files = segment.getPath().toFile().listFiles();
		if (files == null) {
			return 0;
		} else {
			return files.length;
		}
	}

	public List<TestValue> getAll(ReadWriteSegment<TestValue> segment) {
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

	public TimeFrameKey createTimeFrameKey(long start, long end, TestValue obj) {
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

	public HashGroupedKey<?> insertAtId(UUID id, TestValue value) {
		HashGroupedKey<?> key = new UUIDKey(id);
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

	public void insertToHashGroupedCollection(HashGroupedKey<?> key, TestValue value) {
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
			if (value == null) {
				fail();
			}
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
			for (File f : Objects.requireNonNull(folder.listFiles())) {
				if (f.isDirectory()) {
					emptyAndDelete(f);
				} else {
					f.delete();
				}
			}
			folder.delete();
		}
	}

	public void removeKey(BlueKey key) {
		try {
			getTimeCollection().delete(key);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	public TestValue createValue(String name, int cupcakes) {
		return new TestValue(name, cupcakes);
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
		while (iterator.hasNext()) {
			results.add(iterator.next().getValue());
		}
		return results;
	}

	public static <T extends Serializable> List<BlueEntity<T>> toEntityList(Iterator<BlueEntity<T>> iterator) {
		List<BlueEntity<T>> results = new ArrayList<>();
		while (iterator.hasNext()) {
			results.add(iterator.next());
		}
		return results;
	}

	public enum StartupOption {
		EncryptionEnabled,
		EncryptionDisabled
	}
	
	/*
	GETTERS
	 */

	public String getTimeCollectionName() {
		return TIME_COLLECTION_NAME;
	}

	public ReadWriteTimeCollectionOnDisk<TestValue> getTimeCollection() {
		return timeCollection;
	}

	public ReadWriteDbOnDisk getDb() {
		return db;
	}

	public Path getDbPath() {
		return dbPath;
	}

	public LockManager<Path> getLockManager() {
		return lockManager;
	}

	public ReadWriteCollectionOnDisk<TestValue> getHashGroupedCollection() {
		return hashGroupedCollection;
	}

	public ReadWriteCollectionOnDisk<TestValue> getLongCollection() {
		return longCollection;
	}

	public ReadWriteCollectionOnDisk<Call> getCallCollection() {
		return callCollection;
	}

	public ReadWriteCollectionOnDisk<TestValue> getIntCollection() {
		return intCollection;
	}

	public SimpleEncryptionService getEncryptionService() {
		return encryptionService;
	}

	public RollupScheduler getRollupScheduler() {
		return rollupScheduler;
	}

	public ReadWriteCollectionMetaData getTimeCollectionMetaData() {
		return timeCollectionMetaData;
	}

}
