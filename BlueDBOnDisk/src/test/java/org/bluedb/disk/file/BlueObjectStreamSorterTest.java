package org.bluedb.disk.file;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.config.TestDefaultConfigurationService;
import org.bluedb.disk.encryption.EncryptionServiceWrapper;
import org.bluedb.disk.file.BlueObjectStreamSorter.BlueObjectStreamSorterConfig;
import org.bluedb.disk.metadata.BlueFileMetadataKey;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.recovery.OnDiskSortedChangeSupplier;
import org.bluedb.disk.recovery.SortedChangeIterator;
import org.bluedb.disk.recovery.SortedChangeSupplier;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BlueObjectStreamSorterTest {
	
	private Path tmpDir;
	private Path tmpPath;
	private BlueSerializer serializer;
	private EncryptionServiceWrapper encryptionService;
	private ReadWriteFileManager fileManager;
	private Map<BlueFileMetadataKey, String> metadataEntries;
	
	@Before
	public void before() throws IOException {
		tmpDir = Files.createTempDirectory("BlueObjectStreamSorterTest");
		tmpDir.toFile().deleteOnExit();
		tmpPath = tmpDir.resolve("results");
		
		serializer = new ThreadLocalFstSerializer(new TestDefaultConfigurationService());
		encryptionService = new EncryptionServiceWrapper(null);
		fileManager = new ReadWriteFileManager(serializer, encryptionService);
		
		metadataEntries = new HashMap<>();
		metadataEntries.put(BlueFileMetadataKey.SORTED_MASS_CHANGE_FILE, String.valueOf(true));
	}
	
	@After
	public void after() {
		Blutils.recursiveDelete(tmpDir.toFile());
	}

	@Test
	public void test_sortSmallBatchWithIterator() throws BlueDbException {
		testUsingIterator(50, -1);
	}

	@Test
	public void test_sortSmallBatchWithoutIterator() throws BlueDbException {
		testWithoutIterator(50, 1000, -1);
	}

	@Test
	public void test_sortSmallBatchWithIteratorAndSingleBatch() throws BlueDbException {
		testUsingIteratorPlusBatch(50, 13, -1);
	}

	@Test
	public void test_nullItemSkippedInSmallBatchWithIterator() throws BlueDbException {
		testUsingIterator(50, 10);
	}

	@Test
	public void test_nullItemSkippedInSmallBatchWithoutIterator() throws BlueDbException {
		testWithoutIterator(50, 1000, 10);
	}

	@Test
	public void test_nullItemSkippedInSmallBatchWithIteratorAndSingleBatch() throws BlueDbException {
		testUsingIteratorPlusBatch(50, 13, 10);
	}

	@Test
	public void test_sortLargeBatchWithIterator() throws BlueDbException {
		testUsingIterator(11000, -1);
	}

	@Test
	public void test_sortLargeBatchWithoutIterator() throws BlueDbException {
		testWithoutIterator(11000, 1000, -1);
	}

	@Test
	public void test_outputPathCannotBeWrittenTo() throws BlueDbException, IOException {
		Files.createFile(tmpPath);
		tmpPath.toFile().setReadOnly();
		try {
			testUsingIterator(11000, -1);
			fail();
		} catch(BlueDbException e) {
			//expected
		}
	}

	@Test
	public void test_sortLargeBatchWithIteratorAndSingleBatch() throws BlueDbException {
		testUsingIteratorPlusBatch(11000, 13, -1);
	}

	@Test
	public void test_sortLargeNonMultipleOf1000BatchWithIterator() throws BlueDbException {
		testUsingIterator(11007, -1);
	}

	@Test
	public void test_sortLargeNonMultipleOf1000BatchWithoutIterator() throws BlueDbException {
		testWithoutIterator(11007, 1000, -1);
	}

	@Test
	public void test_sortLargeNonMultipleOf1000WithIteratorAndSingleBatch() throws BlueDbException {
		testUsingIteratorPlusBatch(11007, 13, -1);
	}

	private void testUsingIterator(int itemCount, int nullIndex) throws BlueDbException {
		List<IndividualChange<TestValue>> unsortedChangesList = new ArrayList<>();
		for(int i = 0; i < itemCount; i++) {
			if(i != nullIndex) {
				BlueKey key = new TimeKey(0, i);
				TestValue oldValue = new TestValue(String.valueOf(i), i);
				TestValue newValue = new TestValue(String.valueOf(i), i+1);
				unsortedChangesList.add(IndividualChange.createChange(key, oldValue, newValue));
			} else {
				unsortedChangesList.add(null);
			}
		}
		Collections.shuffle(unsortedChangesList);

		BlueObjectStreamSorterConfig config = new BlueObjectStreamSorterConfig(100, 10, 10, 5);
		BlueObjectStreamSorter<IndividualChange<TestValue>> sorter = new BlueObjectStreamSorter<>(unsortedChangesList.iterator(), tmpPath, fileManager, metadataEntries, config);
		sorter.sortAndWriteToFile();
		
		int expectedCount = (nullIndex >= 0) ? itemCount - 1 : itemCount;
		
		validateSortedFile(expectedCount);
	}

	private void testUsingIteratorPlusBatch(int itemCount, int batchSize, int nullIndex) throws BlueDbException {
		List<IndividualChange<TestValue>> unsortedBatchChangesList = new ArrayList<>();
		
		for(int i = 0; i < batchSize; i++) {
			if(i != nullIndex) {
				BlueKey key = new TimeKey(0, i);
				TestValue oldValue = new TestValue(String.valueOf(i), i);
				TestValue newValue = new TestValue(String.valueOf(i), i+1);
				unsortedBatchChangesList.add(IndividualChange.createChange(key, oldValue, newValue));
			} else {
				unsortedBatchChangesList.add(null);
			}
		}
		Collections.shuffle(unsortedBatchChangesList);
		
		List<IndividualChange<TestValue>> unsortedChangesList = new ArrayList<>();
		for(int i = batchSize; i < itemCount; i++) {
			if(i != nullIndex) {
				BlueKey key = new TimeKey(0, i);
				TestValue oldValue = new TestValue(String.valueOf(i), i);
				TestValue newValue = new TestValue(String.valueOf(i), i+1);
				unsortedChangesList.add(IndividualChange.createChange(key, oldValue, newValue));
			} else {
				unsortedChangesList.add(null);
			}
		}
		Collections.shuffle(unsortedChangesList);
		
		BlueObjectStreamSorterConfig config = new BlueObjectStreamSorterConfig(100, 10, 10, 5);
		BlueObjectStreamSorter<IndividualChange<TestValue>> sorter = new BlueObjectStreamSorter<>(unsortedChangesList.iterator(), tmpPath, fileManager, metadataEntries, config);
		sorter.addBatchOfObjectsToBeSorted(unsortedBatchChangesList);
		sorter.sortAndWriteToFile();
		
		int expectedCount = (nullIndex >= 0) ? itemCount - 1 : itemCount;
		
		validateSortedFile(expectedCount);
	}

	private void testWithoutIterator(int itemCount, int batchSize, int nullIndex) throws BlueDbException {
		BlueObjectStreamSorterConfig config = new BlueObjectStreamSorterConfig(100, 10, 10, 5);
		BlueObjectStreamSorter<IndividualChange<TestValue>> sorter = new BlueObjectStreamSorter<>(tmpPath, fileManager, metadataEntries, config);		
		
		List<IndividualChange<TestValue>> unsortedChangesList = new ArrayList<>();
		for(int i = 0; i < itemCount; i++) {
			if(i != nullIndex) {
				BlueKey key = new TimeKey(0, i);
				TestValue oldValue = new TestValue(String.valueOf(i), i);
				TestValue newValue = new TestValue(String.valueOf(i), i+1);
				unsortedChangesList.add(IndividualChange.createChange(key, oldValue, newValue));
			} else {
				unsortedChangesList.add(null);
			}
			
			if(unsortedChangesList.size() >= batchSize) {
				Collections.shuffle(unsortedChangesList);
				sorter.addBatchOfObjectsToBeSorted(unsortedChangesList);
				unsortedChangesList.clear();
			}
		}
		
		if(!unsortedChangesList.isEmpty()) {
			Collections.shuffle(unsortedChangesList);
			sorter.addBatchOfObjectsToBeSorted(unsortedChangesList);
			unsortedChangesList.clear();
		}
		
		sorter.sortAndWriteToFile();
		
		int expectedCount = (nullIndex >= 0) ? itemCount - 1 : itemCount;
		
		validateSortedFile(expectedCount);
	}

	private void validateSortedFile(int expectedCount) throws BlueDbException {
		SortedChangeSupplier<TestValue> changeSupplier = new OnDiskSortedChangeSupplier<>(tmpPath, fileManager);
		SortedChangeIterator<TestValue> changeIterator = new SortedChangeIterator<>(changeSupplier);
		int i = 0;
		IndividualChange<TestValue> previousChange = null;
		while(changeIterator.hasNext()) {
			IndividualChange<TestValue> next = changeIterator.next();
			if(previousChange != null) {
				assertTrue(previousChange.getKey().compareTo(next.getKey()) < 0);
			}
			previousChange = next;
			i++;
		}
		assertEquals(expectedCount, i);
	}
	
}
