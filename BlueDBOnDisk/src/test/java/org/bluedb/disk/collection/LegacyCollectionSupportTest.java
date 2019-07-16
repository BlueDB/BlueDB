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
import org.bluedb.api.index.BlueIndex;
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
import org.bluedb.disk.IndexableTestValue.IndexableTestValueLongIndexExtractor;
import org.bluedb.disk.IndexableTestValue.IndexableTestValueIntIndexExtractor;
import org.bluedb.disk.IndexableTestValue.IndexableTestValueStringIndexExtractor;
import org.bluedb.disk.IndexableTestValue.IndexableTestValueUUIDIndexExtractor;
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
	
	private BlueIndex<LongKey, IndexableTestValue> longIndex;
	private BlueIndex<IntegerKey, IndexableTestValue> intIndex;
	private BlueIndex<StringKey, IndexableTestValue> stringIndex;
	private BlueIndex<UUIDKey, IndexableTestValue> uuidIndex;
	
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
		
		longIndex = timeCollection.createIndex("long-index", LongKey.class, new IndexableTestValueLongIndexExtractor());
		intIndex = timeCollection.createIndex("int-index", IntegerKey.class, new IndexableTestValueIntIndexExtractor());
		stringIndex = timeCollection.createIndex("string-index", StringKey.class, new IndexableTestValueStringIndexExtractor());
		uuidIndex = timeCollection.createIndex("uuid-index", UUIDKey.class, new IndexableTestValueUUIDIndexExtractor());
		
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
		assertValues();
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
