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
import org.bluedb.disk.encryption.EncryptionServiceWrapper;
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
		
		serializer = new ThreadLocalFstSerializer();
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
	public void test_sortSmallBatch() throws BlueDbException {
		testBatch(50, -1);
	}

	@Test
	public void test_sortLargeBatch() throws BlueDbException {
		testBatch(11000, -1);
	}

	@Test
	public void test_nullItemSkippedInSort() throws BlueDbException {
		testBatch(11000, 10);
	}

	private void testBatch(int batchSize, int nullIndex) throws BlueDbException {
		List<IndividualChange<TestValue>> unsortedChangesList = new ArrayList<>();
		for(int i = 0; i < batchSize; i++) {
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
		
		BlueObjectStreamSorter<IndividualChange<TestValue>> sorter = new BlueObjectStreamSorter<>(unsortedChangesList.iterator(), tmpPath, fileManager, metadataEntries);
		sorter.sortAndWriteToFile();
		
		int expectedCount = (nullIndex >= 0) ? batchSize - 1 : batchSize;
		
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

	@Test
	public void test_nullInputThrowsException() {
		BlueObjectStreamSorter<IndividualChange<TestValue>> sorter = new BlueObjectStreamSorter<>(null, tmpPath, fileManager, metadataEntries);
		try {
			sorter.sortAndWriteToFile();
			fail();
		} catch (BlueDbException e) {
			//expected
		}
	}
	
}
