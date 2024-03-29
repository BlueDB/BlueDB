package org.bluedb.disk.collection;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.bluedb.TestUtils;
import org.bluedb.api.BlueCollection;
import org.bluedb.api.SegmentSize;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.api.keys.UUIDKey;
import org.bluedb.disk.BlueDbOnDiskBuilder;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.IndexableTestValue;
import org.bluedb.disk.IndexableTestValue.IndexableTestValueIntIndexExtractor;
import org.bluedb.disk.IndexableTestValue.IndexableTestValueLongIndexExtractor;
import org.bluedb.disk.IndexableTestValue.IndexableTestValueStringIndexExtractor;
import org.bluedb.disk.IndexableTestValue.IndexableTestValueUUIDIndexExtractor;
import org.bluedb.disk.ReadWriteDbOnDisk;
import org.bluedb.disk.collection.index.ReadWriteIndexOnDisk;
import org.bluedb.disk.file.FileUtils;
import org.bluedb.disk.segment.SegmentSizeSetting;
import org.bluedb.zip.ZipUtils;
import org.junit.Test;

import junit.framework.TestCase;

public class LegacyCollectionSupportTest extends TestCase {
	private final String DB_NAME = "legacy-bluedb-instance";
	
	private Path backupPath;
	private Path tmpDirPath;
	private Path dbPath;
	
	private ReadWriteDbOnDisk db;
	
	private List<BlueCollection<IndexableTestValue>> allCollections;
	private ReadWriteTimeCollectionOnDisk<IndexableTestValue> timeframeCollection;
	private ReadWriteTimeCollectionOnDisk<IndexableTestValue> timeCollection;
	private ReadWriteCollectionOnDisk<IndexableTestValue> intCollection;
	private ReadWriteCollectionOnDisk<IndexableTestValue> longCollection;
	private ReadWriteCollectionOnDisk<IndexableTestValue> stringCollection;
	private ReadWriteCollectionOnDisk<IndexableTestValue> uuidCollection;
	
	private ReadWriteIndexOnDisk<LongKey, IndexableTestValue> longIndex;
	private ReadWriteIndexOnDisk<IntegerKey, IndexableTestValue> intIndex;
	private ReadWriteIndexOnDisk<StringKey, IndexableTestValue> stringIndex;
	private ReadWriteIndexOnDisk<UUIDKey, IndexableTestValue> uuidIndex;
	
	private List<Long> existingValuesToTest = Arrays.asList(
			0L,
			1000L,
			10_000L,
			10_010L,
			10_100L,
			100_000L,
			1_000_000L,
			1_200_000L,
			1_400_000L,
			10_000_000L,
			100_100_000L,
			100_200_000L,
			100_500_000L
	);
	
	private List<Long> newValuesToTest;
	
	private List<Long> allValuesToTest; 
	
	private List<File> filesToDelete;

	@SuppressWarnings("deprecation")
	@Override
	protected void setUp() throws Exception {
		filesToDelete = new ArrayList<>();
		
		backupPath = TestUtils.getResourcePath(DB_NAME + ".zip");
		tmpDirPath = createTempFolder().toPath();
		dbPath = tmpDirPath.resolve(DB_NAME);
		
		if(FileUtils.exists(backupPath)) {
			ZipUtils.extractFiles(backupPath, tmpDirPath);
		}
		
		db = (ReadWriteDbOnDisk) new BlueDbOnDiskBuilder()
				.setPath(dbPath)
				.build();
		
		timeframeCollection = (ReadWriteTimeCollectionOnDisk<IndexableTestValue>) db.getTimeCollectionBuilder("timeframe-collection", TimeFrameKey.class, IndexableTestValue.class)
				.withOptimizedClasses(Arrays.asList(UUID.class, IndexableTestValue.class))
				.withSegmentSize(SegmentSize.TIME_FRAME_1_MONTH)
				.build();
		
		timeCollection = (ReadWriteTimeCollectionOnDisk<IndexableTestValue>) db.getTimeCollectionBuilder("time-collection", TimeKey.class, IndexableTestValue.class)
				.withOptimizedClasses(Arrays.asList(UUID.class, IndexableTestValue.class))
				.withSegmentSize(SegmentSize.TIME_1_MONTH)
				.build();
		
		intCollection = (ReadWriteCollectionOnDisk<IndexableTestValue>) db.getCollectionBuilder("int-collection", IntegerKey.class, IndexableTestValue.class)
				.withOptimizedClasses(Arrays.asList(UUID.class, IndexableTestValue.class))
				.withSegmentSize(SegmentSize.INT_1K)
				.build();
		
		longCollection = (ReadWriteCollectionOnDisk<IndexableTestValue>) db.getCollectionBuilder("long-collection", LongKey.class, IndexableTestValue.class)
				.withOptimizedClasses(Arrays.asList(UUID.class, IndexableTestValue.class))
				.withSegmentSize(SegmentSize.LONG_1K)
				.build();
		
		stringCollection = (ReadWriteCollectionOnDisk<IndexableTestValue>) db.getCollectionBuilder("string-collection", StringKey.class, IndexableTestValue.class)
				.withOptimizedClasses(Arrays.asList(UUID.class, IndexableTestValue.class))
				.withSegmentSize(SegmentSize.STRING_4M)
				.build();
		
		uuidCollection = (ReadWriteCollectionOnDisk<IndexableTestValue>) db.getCollectionBuilder("uuid-collection", UUIDKey.class, IndexableTestValue.class)
				.withOptimizedClasses(Arrays.asList(UUID.class, IndexableTestValue.class))
				.withSegmentSize(SegmentSize.UUID_4M)
				.build();
		
		allCollections = Arrays.asList(timeframeCollection, timeCollection, intCollection, longCollection, stringCollection, uuidCollection);
		
		longIndex = (ReadWriteIndexOnDisk<LongKey, IndexableTestValue>) timeCollection.createIndex("long-index", LongKey.class, new IndexableTestValueLongIndexExtractor());
		intIndex = (ReadWriteIndexOnDisk<IntegerKey, IndexableTestValue>) timeCollection.createIndex("int-index", IntegerKey.class, new IndexableTestValueIntIndexExtractor());
		stringIndex = (ReadWriteIndexOnDisk<StringKey, IndexableTestValue>) timeCollection.createIndex("string-index", StringKey.class, new IndexableTestValueStringIndexExtractor());
		uuidIndex =(ReadWriteIndexOnDisk<UUIDKey, IndexableTestValue>) timeCollection.createIndex("uuid-index", UUIDKey.class, new IndexableTestValueUUIDIndexExtractor());
		
		newValuesToTest = existingValuesToTest.stream()
				.map(l -> l+1)
				.collect(Collectors.toCollection(ArrayList::new));
		
		allValuesToTest = new ArrayList<>(existingValuesToTest);
		allValuesToTest.addAll(newValuesToTest);
		
		if(!FileUtils.exists(backupPath)) {
			insertValues(existingValuesToTest);
			createBackup();
		}
		
		insertValues(newValuesToTest);
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

	private void insertValues(List<Long> values) throws BlueDbException {
		for(Long value : values) {
			insertValue(value);
		}
	}

	private void insertValue(long longValue) throws BlueDbException {
		IndexableTestValue value = createValueFromLong(longValue);
		timeframeCollection.insert(value.getTimeFrameKey(), value);
		timeCollection.insert(value.getTimeKey(), value);
		intCollection.insert(value.getIntegerKey(), value);
		longCollection.insert(value.getLongKey(), value);
		stringCollection.insert(value.getStringKey(), value);
		uuidCollection.insert(value.getUUIDKey(), value);
	}

	private IndexableTestValue createValueFromLong(long longValue) {
		return new IndexableTestValue(new UUID(longValue, longValue), longValue, longValue + 10_000_000, Long.toString(longValue), (int) longValue);
	}

	private void createBackup() throws BlueDbException, IOException {
		ZipUtils.zipFile(dbPath, backupPath);
	}
	
	@Test
	public void test() throws BlueDbException {
		assertSegmentSizes();
		assertValues();
	}
	
	private void assertSegmentSizes() throws BlueDbException {
		assertEquals(SegmentSizeSetting.getOriginalDefaultSettingsFor(TimeFrameKey.class).getSegmentSize(), timeframeCollection.getSegmentManager().getSegmentSize());
		assertEquals(SegmentSizeSetting.getOriginalDefaultSettingsFor(TimeKey.class).getSegmentSize(), timeCollection.getSegmentManager().getSegmentSize());
		assertEquals(SegmentSizeSetting.getOriginalDefaultSettingsFor(IntegerKey.class).getSegmentSize(), intCollection.getSegmentManager().getSegmentSize());
		assertEquals(SegmentSizeSetting.getOriginalDefaultSettingsFor(LongKey.class).getSegmentSize(), longCollection.getSegmentManager().getSegmentSize());
		assertEquals(SegmentSizeSetting.getOriginalDefaultSettingsFor(StringKey.class).getSegmentSize(), stringCollection.getSegmentManager().getSegmentSize());
		assertEquals(SegmentSizeSetting.getOriginalDefaultSettingsFor(UUIDKey.class).getSegmentSize(), uuidCollection.getSegmentManager().getSegmentSize());
		
		assertEquals(SegmentSizeSetting.getOriginalDefaultSettingsFor(LongKey.class).getSegmentSize(), longIndex.getSegmentManager().getSegmentSize());
		assertEquals(SegmentSizeSetting.getOriginalDefaultSettingsFor(IntegerKey.class).getSegmentSize(), intIndex.getSegmentManager().getSegmentSize());
		assertEquals(SegmentSizeSetting.getOriginalDefaultSettingsFor(StringKey.class).getSegmentSize(), stringIndex.getSegmentManager().getSegmentSize());
		assertEquals(SegmentSizeSetting.getOriginalDefaultSettingsFor(UUIDKey.class).getSegmentSize(), uuidIndex.getSegmentManager().getSegmentSize());
	}

	private void assertValues() throws BlueDbException {
		for(BlueCollection<IndexableTestValue> collection : allCollections) {
			assertEquals(allValuesToTest.size(), collection.query().getList().size());
		}
		
		for(Long longValue : allValuesToTest) {
			assertValue(longValue);
		}
	}

	private void assertValue(Long longValue) throws BlueDbException {
		IndexableTestValue value = createValueFromLong(longValue);
		
		TestUtils.assertCollectionAndValue(timeframeCollection, value.getTimeFrameKey(), value);
		TestUtils.assertCollectionAndValue(timeCollection, value.getTimeKey(), value);
		TestUtils.assertCollectionAndValue(intCollection, value.getIntegerKey(), value);
		TestUtils.assertCollectionAndValue(longCollection, value.getLongKey(), value);
		TestUtils.assertCollectionAndValue(stringCollection, value.getStringKey(), value);
		TestUtils.assertCollectionAndValue(uuidCollection, value.getUUIDKey(), value);
		
		assertEquals(true, timeCollection.query()
				.where(longIndex.createLongIndexCondition().isEqualTo(value.getLongKey().getId()))
				.getList()
				.contains(value));
		assertEquals(true, timeCollection.query()
				.where(intIndex.createIntegerIndexCondition().isEqualTo(value.getIntegerKey().getId()))
				.getList()
				.contains(value));
		assertEquals(true, timeCollection.query()
				.where(stringIndex.createStringIndexCondition().isEqualTo(value.getStringKey().getId()))
				.getList()
				.contains(value));
		assertEquals(true, timeCollection.query()
				.where(uuidIndex.createUUIDIndexCondition().isEqualTo(value.getUUIDKey().getId()))
				.getList()
				.contains(value));
	}
}
