package org.bluedb.disk.file;

import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.bluedb.disk.Blutils;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.config.TestDefaultConfigurationService;
import org.bluedb.disk.encryption.EncryptionServiceWrapper;
import org.bluedb.disk.metadata.BlueFileMetadata;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;
import org.junit.Test;

import junit.framework.TestCase;

import static org.junit.Assert.*;

public class ReadFileManagerTest extends TestCase {
	
	BlueSerializer serializer;
	
	private Path testingFolderPath;
	private Path targetFilePath;
	private Path tempFilePath;

	@Override
	protected void setUp() throws Exception {
		testingFolderPath = Files.createTempDirectory(this.getClass().getSimpleName());
		targetFilePath = Paths.get(testingFolderPath.toString(), "ReadFileManagerTest.test_junk");
		tempFilePath = FileUtils.createTempFilePath(targetFilePath);
		serializer = new ThreadLocalFstSerializer(new TestDefaultConfigurationService(), new Class[]{});
	}

	@Override
	public void tearDown() throws Exception {
		targetFilePath.toFile().delete();
		tempFilePath.toFile().delete();
		Blutils.recursiveDelete(targetFilePath.toFile());
		Blutils.recursiveDelete(testingFolderPath.toFile());
	}

	@Test
	public void test_readMetadata_objectSuccessfullyParsesIntoUnexpectedType_returnsNullAndResetsInputStream() throws Exception {
		// Arrange
		TestValue value = new TestValue("Sandra Bowl of Grits");
		byte[] valueBytes = serializer.serializeObjectToByteArray(value);
		try (DataOutputStream outputStream = FileUtils.openDataOutputStream(tempFilePath.toFile())) {
			outputStream.writeInt(valueBytes.length);
			outputStream.write(valueBytes);
		}
		ReadFileManager fileManager = new ReadWriteFileManager(serializer, new EncryptionServiceWrapper(null));

		// Act
		try (BlueInputStream inputStream = new BlueDataInputStream(tempFilePath.toFile())) {
			BlueFileMetadata actual = fileManager.readMetadata(inputStream);

			// Assert
			assertEquals("InputStream position was not reset as expected", (Integer) valueBytes.length, inputStream.readNextFourBytesAsInt());
			assertNull(actual);
		}
	}

	@Test
	public void test_readMetadata_metaDataSizeTooLargeToMakeSense_returnsNullAndResetsInputStream() throws Exception {
		// Arrange
		TestValue value = new TestValue("Sandra Bowl of Grits");
		byte[] valueBytes = serializer.serializeObjectToByteArray(value);
		try (DataOutputStream outputStream = FileUtils.openDataOutputStream(tempFilePath.toFile())) {
			outputStream.writeInt(valueBytes.length + 100);
			outputStream.write(valueBytes);
		}
		ReadFileManager fileManager = new ReadWriteFileManager(serializer, new EncryptionServiceWrapper(null));

		// Act
		try (BlueInputStream inputStream = new BlueDataInputStream(tempFilePath.toFile())) {
			BlueFileMetadata actual = fileManager.readMetadata(inputStream);

			// Assert
			assertEquals("InputStream position was not reset as expected", (Integer) (valueBytes.length + 100), inputStream.readNextFourBytesAsInt());
			assertNull(actual);
		}
	}

}
