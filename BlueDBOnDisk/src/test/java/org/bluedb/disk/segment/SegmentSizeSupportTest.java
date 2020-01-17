package org.bluedb.disk.segment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import org.bluedb.TestUtils;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.IntegerIndexKeyExtractor;
import org.bluedb.api.index.LongIndexKeyExtractor;
import org.bluedb.api.index.StringIndexKeyExtractor;
import org.bluedb.api.index.UUIDIndexKeyExtractor;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.HashGroupedKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.api.keys.UUIDKey;
import org.bluedb.disk.ReadWriteBlueDbOnDisk;
import org.bluedb.disk.BlueDbOnDiskBuilder;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.IndexableTestValue;
import org.bluedb.disk.collection.ReadWriteBlueCollectionOnDisk;
import org.bluedb.disk.collection.index.ReadWriteBlueIndexOnDisk;

public class SegmentSizeSupportTest {
	private Path dbPath;
	
	private ReadWriteBlueDbOnDisk db;
	
	private SegmentSizeSetting segmentSize;
	private ReadWriteBlueCollectionOnDisk<IndexableTestValue> collection;
	
	private ReadWriteBlueIndexOnDisk<LongKey, IndexableTestValue> longIndex;
	private ReadWriteBlueIndexOnDisk<IntegerKey, IndexableTestValue> intIndex;
	private ReadWriteBlueIndexOnDisk<StringKey, IndexableTestValue> stringIndex;
	private ReadWriteBlueIndexOnDisk<UUIDKey, IndexableTestValue> uuidIndex;
	
	private List<Long> valuesToTest = Arrays.asList(
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
	
	private List<File> filesToDelete;
	
	private KeySupplier<? extends BlueKey, IndexableTestValue> keySupplier;
	
	public SegmentSizeSupportTest(SegmentSizeSetting segmentSize) throws Exception {
		filesToDelete = new ArrayList<>();
		
		dbPath = createTempFolder().toPath().resolve("seg-size-support-test");
		
		keySupplier = createKeySupplier(segmentSize.getKeyType());
		
		db = (ReadWriteBlueDbOnDisk) new BlueDbOnDiskBuilder()
				.withPath(dbPath)
				.build();
		
		this.segmentSize = segmentSize;
		collection = new ReadWriteBlueCollectionOnDisk<>(db, "seg-size-support-test-collection", segmentSize.getKeyType(), IndexableTestValue.class, 
				Arrays.asList(UUID.class, IndexableTestValue.class), segmentSize);
		
		LongIndexKeyExtractor<IndexableTestValue> longExtractor = value -> Arrays.asList(value.getLongValue());
		IntegerIndexKeyExtractor<IndexableTestValue> intExtractor = value -> Arrays.asList(value.getIntValue());
		StringIndexKeyExtractor<IndexableTestValue> stringExtractor = value -> Arrays.asList(value.getStringValue());
		UUIDIndexKeyExtractor<IndexableTestValue> uuidExtractor = value -> Arrays.asList(value.getId());
		
		longIndex = (ReadWriteBlueIndexOnDisk<LongKey, IndexableTestValue>) collection.createIndex("long-index", LongKey.class, longExtractor);
		intIndex = (ReadWriteBlueIndexOnDisk<IntegerKey, IndexableTestValue>) collection.createIndex("int-index", IntegerKey.class, intExtractor);
		stringIndex = (ReadWriteBlueIndexOnDisk<StringKey, IndexableTestValue>) collection.createIndex("string-index", StringKey.class, stringExtractor);
		uuidIndex = (ReadWriteBlueIndexOnDisk<UUIDKey, IndexableTestValue>) collection.createIndex("uuid-index", UUIDKey.class, uuidExtractor);
		
		insertValues(valuesToTest);
	}

	private KeySupplier<? extends BlueKey, IndexableTestValue> createKeySupplier(Class<? extends BlueKey> keyType) throws BlueDbException {
		if (TimeFrameKey.class.isAssignableFrom(keyType)) {
			return value -> value.getTimeFrameKey();
		} else if (TimeKey.class.isAssignableFrom(keyType)) {
			return value -> value.getTimeKey();
		} else if (LongKey.class.isAssignableFrom(keyType)) {
			return value -> value.getLongKey();
		} else if (IntegerKey.class.isAssignableFrom(keyType)) {
			return value -> value.getIntegerKey();
		} else if (HashGroupedKey.class.isAssignableFrom(keyType)) {
			return value -> value.getUUIDKey();
		} else {
			throw new BlueDbException("No key supplier for key type " + keyType);
		}
	}

	public void cleanup() throws Exception {
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
		collection.insert(keySupplier.createKey(value), value);
	}

	private IndexableTestValue createValueFromLong(long longValue) {
		return new IndexableTestValue(new UUID(longValue, longValue), longValue, longValue + 10_000_000, Long.toString(longValue), (int) longValue);
	}

	public void testCollectionAndIndices() throws BlueDbException {
		assertSegmentSizes();
		assertValues();
	}
	
	private void assertSegmentSizes() throws BlueDbException {
		assertEquals(segmentSize.getSegmentSize(), collection.getSegmentManager().getSegmentSize());
	}

	private void assertValues() throws BlueDbException {
		assertEquals(valuesToTest.size(), collection.query().getList().size());
		
		for(Long longValue : valuesToTest) {
			assertValue(longValue);
		}
	}

	private void assertValue(Long longValue) throws BlueDbException {
		IndexableTestValue value = createValueFromLong(longValue);
		
		TestUtils.assertCollectionAndValue(collection, keySupplier.createKey(value), value);
		
		assertEquals(true, longIndex.get(value.getLongKey()).contains(value));
		assertEquals(true, intIndex.get(value.getIntegerKey()).contains(value));
		assertEquals(true, stringIndex.get(value.getStringKey()).contains(value));
		assertEquals(true, uuidIndex.get(value.getUUIDKey()).contains(value));
	}
	
	@FunctionalInterface
	private static interface KeySupplier<K, V> {
		public K createKey(V value);
	}
}
