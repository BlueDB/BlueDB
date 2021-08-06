package org.bluedb.disk.encryption;

import java.util.Optional;

import org.bluedb.disk.metadata.BlueFileMetadata;
import org.bluedb.disk.metadata.BlueFileMetadataKey;
import org.junit.Test;
import static org.junit.Assert.*;

public class EncryptionUtilsTest {

	@Test
	public void test_constructor() {
		new EncryptionUtils(); // this doesn't really test anything, it just makes code coverage 100%
	}

	@Test
	public void test_getEncryptionVersionKey_nullMetadata_returnsEmptyOptional() {
		// Arrange
		Optional<String> expected = Optional.empty();
		// Act
		Optional<String> actual = EncryptionUtils.getEncryptionVersionKey(null);
		// Assert
		assertEquals(expected, actual);
	}

	@Test
	public void test_getEncryptionVersionKey_EmptyMetadata_returnsEmptyOptional() {
		// Arrange
		Optional<String> expected = Optional.empty();
		BlueFileMetadata blueFileMetadata = new BlueFileMetadata();
		// Act
		Optional<String> actual = EncryptionUtils.getEncryptionVersionKey(blueFileMetadata);
		// Assert
		assertEquals(expected, actual);
	}

	@Test
	public void test_getEncryptionVersionKey_MetadataContainsKey_returnsKeyFromMetadata() {
		// Arrange
		String key = "valid-key";
		Optional<String> expected = Optional.of(key);
		BlueFileMetadata blueFileMetadata = new BlueFileMetadata();
		blueFileMetadata.put(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY, key);
		// Act
		Optional<String> actual = EncryptionUtils.getEncryptionVersionKey(blueFileMetadata);
		// Assert
		assertEquals(expected, actual);
	}

	@Test
	public void test_isValidEncryptionVersionKey_keyLargerThanMaxLength_returnsFalse() {
		// Arrange
		String key = "21-characters-long!!!";
		// Act
		boolean actual = EncryptionUtils.isValidEncryptionVersionKey(key);
		// Assert
		assertFalse(actual);
	}

	@Test
	public void test_isValidEncryptionVersionKey_keyIsMaxLength_returnsTrue() {
		// Arrange
		String key = "20-characters-long!!";
		// Act
		boolean actual = EncryptionUtils.isValidEncryptionVersionKey(key);
		// Assert
		assertTrue(actual);
	}

	@Test
	public void test_isValidEncryptionVersionKey_keyIsWhitespace_returnsFalse() {
		// Arrange
		String key = "    ";
		// Act
		boolean actual = EncryptionUtils.isValidEncryptionVersionKey(key);
		// Assert
		assertFalse(actual);
	}

	@Test
	public void test_shouldWriterSkipEncryptionForUnchangedDataUsingRawBytes_oneFileEncrypted_returnsFalse() {
		// Arrange
		BlueFileMetadata metadataOfEncryptedFile = new BlueFileMetadata();
		metadataOfEncryptedFile.put(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY, "valid-key");
		BlueFileMetadata metadataOfUnencryptedFile = new BlueFileMetadata();
		// Act
		boolean actual = EncryptionUtils.shouldWriterSkipEncryptionForUnchangedDataUsingRawBytes(metadataOfEncryptedFile, metadataOfUnencryptedFile);
		boolean actualParamsSwitched = EncryptionUtils.shouldWriterSkipEncryptionForUnchangedDataUsingRawBytes(metadataOfUnencryptedFile, metadataOfEncryptedFile);
		// Assert
		assertFalse(actual);
		assertFalse(actualParamsSwitched);
	}

	@Test
	public void test_shouldWriterSkipEncryptionForUnchangedDataUsingRawBytes_neitherFileEncrypted_returnsTrue() {
		// Arrange
		BlueFileMetadata oldMetadata = new BlueFileMetadata();
		BlueFileMetadata newMetadata = new BlueFileMetadata();
		// Act
		boolean actual = EncryptionUtils.shouldWriterSkipEncryptionForUnchangedDataUsingRawBytes(oldMetadata, newMetadata);
		// Assert
		assertTrue(actual);
	}

	@Test
	public void test_shouldWriterSkipEncryptionForUnchangedDataUsingRawBytes_bothFilesEncryptedWithSameKey_returnsTrue() {
		// Arrange
		BlueFileMetadata oldMetadata = new BlueFileMetadata();
		oldMetadata.put(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY, "valid-key");
		BlueFileMetadata newMetadata = new BlueFileMetadata();
		newMetadata.put(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY, "valid-key");
		// Act
		boolean actual = EncryptionUtils.shouldWriterSkipEncryptionForUnchangedDataUsingRawBytes(oldMetadata, newMetadata);
		// Assert
		assertTrue(actual);
	}

	@Test
	public void test_shouldWriterSkipEncryptionForUnchangedDataUsingRawBytes_bothFilesEncryptedWithDifferentKeys_returnsFalse() {
		// Arrange
		BlueFileMetadata oldMetadata = new BlueFileMetadata();
		oldMetadata.put(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY, "valid-key-v1");
		BlueFileMetadata newMetadata = new BlueFileMetadata();
		newMetadata.put(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY, "valid-key-v2");
		// Act
		boolean actual = EncryptionUtils.shouldWriterSkipEncryptionForUnchangedDataUsingRawBytes(oldMetadata, newMetadata);
		// Assert
		assertFalse(actual);
	}

	@Test
	public void test_shouldWriterSkipEncryptionForUnchangedDataUsingRawBytes_bothFilesNull_returnsTrue() {
		// Arrange
		BlueFileMetadata metadataOfUnencryptedFile1 = null;
		BlueFileMetadata metadataOfUnencryptedFile2 = null;
		// Act
		boolean actual = EncryptionUtils.shouldWriterSkipEncryptionForUnchangedDataUsingRawBytes(metadataOfUnencryptedFile1, metadataOfUnencryptedFile2);
		boolean actualParamsSwitched = EncryptionUtils.shouldWriterSkipEncryptionForUnchangedDataUsingRawBytes(metadataOfUnencryptedFile2, metadataOfUnencryptedFile1);
		// Assert
		assertTrue(actual);
		assertTrue(actualParamsSwitched);
	}

	@Test
	public void test_shouldWriterSkipEncryptionForUnchangedDataUsingRawBytes_oneFileNull_returnsFalse() {
		// Arrange
		BlueFileMetadata metadataOfEncryptedFile = new BlueFileMetadata();
		metadataOfEncryptedFile.put(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY, "valid-key");
		BlueFileMetadata metadataOfUnencryptedFile = null;
		// Act
		boolean actual = EncryptionUtils.shouldWriterSkipEncryptionForUnchangedDataUsingRawBytes(metadataOfEncryptedFile, metadataOfUnencryptedFile);
		boolean actualParamsSwitched = EncryptionUtils.shouldWriterSkipEncryptionForUnchangedDataUsingRawBytes(metadataOfUnencryptedFile, metadataOfEncryptedFile);
		// Assert
		assertFalse(actual);
		assertFalse(actualParamsSwitched);
	}

}
