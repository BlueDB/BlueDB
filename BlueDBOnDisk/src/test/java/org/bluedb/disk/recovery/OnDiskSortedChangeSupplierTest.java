package org.bluedb.disk.recovery;

import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import org.bluedb.disk.Blutils;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.config.TestDefaultConfigurationService;
import org.bluedb.disk.encryption.EncryptionService;
import org.bluedb.disk.encryption.EncryptionServiceWrapper;
import org.bluedb.disk.file.BlueObjectOutput;
import org.bluedb.disk.file.ReadFileManager;
import org.bluedb.disk.metadata.BlueFileMetadataKey;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

public class OnDiskSortedChangeSupplierTest extends SortedChangeSupplierTest {
	
	private Path tmpDir;
	private Path supplierFilePath;
	
	private BlueSerializer serializer;
	private EncryptionService encryptionService;
	private EncryptionServiceWrapper encryptionServiceWrapper;
	private ReadFileManager fileManager;
	
	@Before
	public void setup() throws Exception {
		tmpDir = Files.createTempDirectory(getClass().getSimpleName());
		tmpDir.toFile().deleteOnExit();
		
		supplierFilePath = tmpDir.resolve("supplier.bin");
		
		serializer = new ThreadLocalFstSerializer(new TestDefaultConfigurationService());
		
		encryptionService = Mockito.mock(EncryptionService.class);
		when(encryptionService.isEncryptionEnabled()).thenReturn(false);
		
		encryptionServiceWrapper = new EncryptionServiceWrapper(encryptionService);
		
		fileManager = new ReadFileManager(serializer, encryptionServiceWrapper);
		
		super.setup();
	}
	
	@After
	public void after() throws IOException {
		sortedChangeSupplier.close();
		
		Files.walk(tmpDir)
			.sorted(Comparator.reverseOrder())
			.map(Path::toFile)
			.forEach(File::delete);
		Blutils.recursiveDelete(tmpDir.toFile());
	}

	@Override
	protected SortedChangeSupplier<TestValue> createSortedChangeSupplier() throws Exception {
		try (BlueObjectOutput<IndividualChange<TestValue>> output = BlueObjectOutput.createWithoutLock(supplierFilePath, serializer, encryptionServiceWrapper)) {
			output.setMetadataValue(BlueFileMetadataKey.SORTED_MASS_CHANGE_FILE, Boolean.toString(true));
			
			for(IndividualChange<TestValue> change : changeList) {
				output.write(change);
			}
		}
		
		return new OnDiskSortedChangeSupplier<>(supplierFilePath, fileManager); 
	}

	/*
	 * This class creates a specific implementation of SortedChangeSupplier then runs the generic tests on it
	 * which are defined in the parent class. 
	 */
}
