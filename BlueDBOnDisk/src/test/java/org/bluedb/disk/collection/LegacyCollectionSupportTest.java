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
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.IntegerIndexKeyExtractor;
import org.bluedb.api.index.LongIndexKeyExtractor;
import org.bluedb.api.index.StringIndexKeyExtractor;
import org.bluedb.api.index.UUIDIndexKeyExtractor;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.api.keys.UUIDKey;
import org.bluedb.disk.BlueDbOnDisk;
import org.bluedb.disk.BlueDbOnDiskBuilder;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.IndexableTestValue;
import org.bluedb.disk.collection.index.BlueIndexOnDisk;
import org.bluedb.disk.segment.SegmentSizeSetting;
import org.bluedb.zip.ZipUtils;
import org.junit.Test;

import junit.framework.TestCase;

public class LegacyCollectionSupportTest extends TestCase {
	private final String DB_NAME = "legacy-bluedb-instance";
	
	private Path backupPath;
	private Path tmpDirPath;
	private Path dbPath;
	
	private BlueDbOnDisk db;
	
	private List<BlueCollectionOnDisk<IndexableTestValue>> allCollections;
	private BlueCollectionOnDisk<IndexableTestValue> timeframeCollection;
	private BlueCollectionOnDisk<IndexableTestValue> timeCollection;
	private BlueCollectionOnDisk<IndexableTestValue> intCollection;
	private BlueCollectionOnDisk<IndexableTestValue> longCollection;
	private BlueCollectionOnDisk<IndexableTestValue> stringCollection;
	private BlueCollectionOnDisk<IndexableTestValue> uuidCollection;
	
	private BlueIndexOnDisk<LongKey, IndexableTestValue> longIndex;
	private BlueIndexOnDisk<IntegerKey, IndexableTestValue> intIndex;
	private BlueIndexOnDisk<StringKey, IndexableTestValue> stringIndex;
	private BlueIndexOnDisk<UUIDKey, IndexableTestValue> uuidIndex;
	
	private List<Long> existingValuesToTest = Arrays.asList(
		0L,
		1000L,
		10_000L,
		10_005L,
		10_010L,
		10_100L,
		100_000L,
		1_000_000L,
		1_100_000L,
		1_200_000L,
		1_300_000L,
		1_400_000L,
		10_000_000L,
		100_000_000L,
		100_100_000L,
		100_200_000L,
		100_300_000L,
		100_400_000L,
		100_500_000L
	);
	
	private List<Long> newValuesToTest;
	
	private List<Long> allValuesToTest; 
	
	private List<File> filesToDelete;

	@Override
	protected void setUp() throws Exception {
		filesToDelete = new ArrayList<>();
		
		backupPath = TestUtils.getResourcePath(DB_NAME + ".zip");
		tmpDirPath = createTempFolder().toPath();
		dbPath = tmpDirPath.resolve(DB_NAME);
		
		if(Files.exists(backupPath)) {
			ZipUtils.extractFiles(backupPath, tmpDirPath);
		}
		
		db = new BlueDbOnDiskBuilder()
				.setPath(dbPath)
				.build();
		
		timeframeCollection = db.collectionBuilder("timeframe-collection", TimeFrameKey.class, IndexableTestValue.class)
				.withOptimizedClasses(Arrays.asList(UUID.class, IndexableTestValue.class))
				.build();
		
		timeCollection = db.collectionBuilder("time-collection", TimeKey.class, IndexableTestValue.class)
				.withOptimizedClasses(Arrays.asList(UUID.class, IndexableTestValue.class))
				.build();
		
		intCollection = db.collectionBuilder("int-collection", IntegerKey.class, IndexableTestValue.class)
				.withOptimizedClasses(Arrays.asList(UUID.class, IndexableTestValue.class))
				.build();
		
		longCollection = db.collectionBuilder("long-collection", LongKey.class, IndexableTestValue.class)
				.withOptimizedClasses(Arrays.asList(UUID.class, IndexableTestValue.class))
				.build();
		
		stringCollection = db.collectionBuilder("string-collection", StringKey.class, IndexableTestValue.class)
				.withOptimizedClasses(Arrays.asList(UUID.class, IndexableTestValue.class))
				.build();
		
		uuidCollection = db.collectionBuilder("uuid-collection", UUIDKey.class, IndexableTestValue.class)
				.withOptimizedClasses(Arrays.asList(UUID.class, IndexableTestValue.class))
				.build();
		
		allCollections = Arrays.asList(timeframeCollection, timeCollection, intCollection, longCollection, stringCollection, uuidCollection);
		
		LongIndexKeyExtractor<IndexableTestValue> longExtractor = value -> Arrays.asList(value.getLongValue());
		IntegerIndexKeyExtractor<IndexableTestValue> intExtractor = value -> Arrays.asList(value.getIntValue());
		StringIndexKeyExtractor<IndexableTestValue> stringExtractor = value -> Arrays.asList(value.getStringValue());
		UUIDIndexKeyExtractor<IndexableTestValue> uuidExtractor = value -> Arrays.asList(value.getId());
		
		longIndex = (BlueIndexOnDisk<LongKey, IndexableTestValue>) timeCollection.createIndex("long-index", LongKey.class, longExtractor);
		intIndex = (BlueIndexOnDisk<IntegerKey, IndexableTestValue>) timeCollection.createIndex("int-index", IntegerKey.class, intExtractor);
		stringIndex = (BlueIndexOnDisk<StringKey, IndexableTestValue>) timeCollection.createIndex("string-index", StringKey.class, stringExtractor);
		uuidIndex = (BlueIndexOnDisk<UUIDKey, IndexableTestValue>) timeCollection.createIndex("uuid-index", UUIDKey.class, uuidExtractor);
		
		newValuesToTest = existingValuesToTest.stream()
				.map(l -> l+1)
				.collect(Collectors.toCollection(ArrayList::new));
		
		allValuesToTest = new ArrayList<>(existingValuesToTest);
		allValuesToTest.addAll(newValuesToTest);
		
		if(!Files.exists(backupPath)) {
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
		for(BlueCollectionOnDisk<IndexableTestValue> collection : allCollections) {
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
		
		assertEquals(true, longIndex.get(value.getLongKey()).contains(value));
		assertEquals(true, intIndex.get(value.getIntegerKey()).contains(value));
		assertEquals(true, stringIndex.get(value.getStringKey()).contains(value));
		assertEquals(true, uuidIndex.get(value.getUUIDKey()).contains(value));
	}
}
