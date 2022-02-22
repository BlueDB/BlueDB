package org.bluedb.disk.file;

import java.io.DataOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.bluedb.disk.TestValue;
import org.bluedb.disk.encryption.EncryptionServiceWrapper;
import org.bluedb.disk.metadata.BlueFileMetadata;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;
import org.junit.Test;
import static org.junit.Assert.*;

public class ReadFileManagerTest {

	@Test
	public void test_readMetadata_objectSuccessfullyParsesIntoUnexpectedType_returnsNullAndResetsInputStream() throws Exception {
		// Arrange
		Path testPath = Paths.get(".", "test_" + this.getClass().getSimpleName());
		BlueSerializer serializer = new ThreadLocalFstSerializer();
		TestValue value = new TestValue("Sandra Bowl of Grits");
		byte[] valueBytes = serializer.serializeObjectToByteArray(value);
		try (DataOutputStream outputStream = FileUtils.openDataOutputStream(testPath.toFile())) {
			outputStream.writeInt(valueBytes.length);
			outputStream.write(valueBytes);
		}
		ReadFileManager fileManager = new ReadWriteFileManager(serializer, new EncryptionServiceWrapper(null));

		// Act
		try (BlueInputStream inputStream = new BlueDataInputStream(testPath.toFile())) {
			BlueFileMetadata actual = fileManager.readMetadata(inputStream);

			// Assert
			assertEquals("InputStream position was not reset as expected", (Integer) valueBytes.length, inputStream.readNextFourBytesAsInt());
			assertNull(actual);
		}
	}

}
