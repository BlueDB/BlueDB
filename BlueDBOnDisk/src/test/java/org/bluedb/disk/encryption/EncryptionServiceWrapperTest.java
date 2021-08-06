package org.bluedb.disk.encryption;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.bluedb.disk.metadata.BlueFileMetadata;
import org.bluedb.disk.metadata.BlueFileMetadataKey;
import org.junit.Test;
import org.mockito.Mockito;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class EncryptionServiceWrapperTest {

	@Test
	public void test_encryptionServiceWrapper_validEncryptionService_cachesVersionKeySuccessfully() {
		// Arrange
		String expected = "valid-key";
		EncryptionService mockEncryptionService = Mockito.mock(EncryptionService.class);
		when(mockEncryptionService.isEncryptionEnabled()).thenReturn(true);
		when(mockEncryptionService.getCurrentEncryptionVersionKey())
				.thenReturn(expected);
		// Act
		EncryptionServiceWrapper encryptionServiceWrapper = new EncryptionServiceWrapper(mockEncryptionService);
		String actual = encryptionServiceWrapper.cachedEncryptionVersionKey;
		// Assert
		assertEquals(expected, actual);
	}

	@Test
	public void test_encryptionServiceWrapper_invalidEncryptionVersionKey_shouldThrowException() {
		// Arrange
		EncryptionService mockEncryptionService = Mockito.mock(EncryptionService.class);
		when(mockEncryptionService.isEncryptionEnabled()).thenReturn(true);
		when(mockEncryptionService.getCurrentEncryptionVersionKey())
				.thenReturn(null);
		String expectedErrorMsg = "getCurrentEncryptionVersionKey cannot be null or whitespace and must be no longer than " + EncryptionUtils.ENCRYPTION_VERSION_KEY_MAX_LENGTH + " characters.";
		// Act
		try {
			new EncryptionServiceWrapper(mockEncryptionService);
			fail("Expected exception was not thrown");
		} catch (IllegalStateException ex) {
			// Assert
			assertEquals(expectedErrorMsg, ex.getMessage());
		}
	}

	@Test
	public void test_isEncryptionEnabled_nullEncryptionService_shouldReturnFalse() {
		// Arrange
		EncryptionServiceWrapper encryptionServiceWrapper = new EncryptionServiceWrapper(null);
		// Act
		boolean actual = encryptionServiceWrapper.isEncryptionEnabled();
		// Assert
		assertFalse(actual);
	}

	@Test
	public void test_isEncryptionEnabled_EnabledEncryptionService_shouldReturnTrue() {
		// Arrange
		EncryptionService mockEncryptionService = Mockito.mock(EncryptionService.class);
		when(mockEncryptionService.isEncryptionEnabled()).thenReturn(true);
		when(mockEncryptionService.getCurrentEncryptionVersionKey())
				.thenReturn("valid-key");
		EncryptionServiceWrapper encryptionServiceWrapper = new EncryptionServiceWrapper(mockEncryptionService);
		// Act
		boolean actual = encryptionServiceWrapper.isEncryptionEnabled();
		// Assert
		assertTrue(actual);
	}

	@Test
	public void test_getCurrentEncryptionVersionKey_nullEncryptionService_shouldReturnNull() {
		// Arrange
		EncryptionServiceWrapper encryptionServiceWrapper = new EncryptionServiceWrapper(null);
		// Act
		String actual = encryptionServiceWrapper.getCurrentEncryptionVersionKey();
		// Assert
		assertNull(actual);
	}

	@Test
	public void test_getCurrentEncryptionVersionKey_EnabledEncryptionService_shouldReturnExpectedKey() {
		// Arrange
		String expected = "valid-key";
		EncryptionService mockEncryptionService = Mockito.mock(EncryptionService.class);
		when(mockEncryptionService.isEncryptionEnabled()).thenReturn(true);
		when(mockEncryptionService.getCurrentEncryptionVersionKey())
				.thenReturn(expected);
		EncryptionServiceWrapper encryptionServiceWrapper = new EncryptionServiceWrapper(mockEncryptionService);
		// Act
		String actual = encryptionServiceWrapper.getCurrentEncryptionVersionKey();
		// Assert
		assertEquals(expected, actual);
	}

	@Test
	public void test_getCurrentEncryptionVersionKey_keyChangedToInvalid_shouldReturnCachedKeyAndLogWarning() {
		// Arrange
		PrintStream standardOut = System.out;
		ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();
		System.setOut(new PrintStream(outputStreamCaptor));

		String expected = "cached-key";
		String expectedWarningMsg = "Warning: current encryption version key is invalid, this should be fixed ASAP! Using most recent valid key.";
		EncryptionService mockEncryptionService = Mockito.mock(EncryptionService.class);
		when(mockEncryptionService.isEncryptionEnabled()).thenReturn(true);
		when(mockEncryptionService.getCurrentEncryptionVersionKey())
				.thenReturn(expected) // First call
				.thenReturn(null); // All subsequent calls
		EncryptionServiceWrapper encryptionServiceWrapper = new EncryptionServiceWrapper(mockEncryptionService);

		// Act
		String actual = encryptionServiceWrapper.getCurrentEncryptionVersionKey();

		// Assert
		assertEquals(expected, actual);
		assertEquals(expectedWarningMsg, outputStreamCaptor.toString().trim());

		// Cleanup
		System.setOut(standardOut);
	}

	@Test
	public void test_encryptOrThrow_validEncryptionService_shouldReturnResultOfWrappedMethod() {
		// Arrange
		byte[] expected = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
		EncryptionService mockEncryptionService = mock(EncryptionService.class);
		when(mockEncryptionService.isEncryptionEnabled()).thenReturn(true);
		when(mockEncryptionService.getCurrentEncryptionVersionKey())
				.thenReturn("valid-key");
		when(mockEncryptionService.encrypt(anyString(), anyObject()))
				.thenReturn(expected);
		EncryptionServiceWrapper encryptionServiceWrapper = new EncryptionServiceWrapper(mockEncryptionService);

		// Act
		byte[] actual = encryptionServiceWrapper.encryptOrThrow("valid-key", new byte[0]);

		// Assert
		assertArrayEquals(expected, actual);
	}

	@Test
	public void test_encryptOrThrow_nullEncryptionService_shouldThrowException() {
		// Arrange
		EncryptionServiceWrapper encryptionServiceWrapper = new EncryptionServiceWrapper(null);
		String expectedErrorMsg = "Unable to encrypt, encryption service not supplied";
		// Act
		try {
			encryptionServiceWrapper.encryptOrThrow("valid-key", new byte[0]);
			fail("Expected exception was not thrown");
		} catch (IllegalStateException ex) {
			// Assert
			assertEquals(expectedErrorMsg, ex.getMessage());
		}
	}

	@Test
	public void test_decryptOrReturn_encryptedFile_shouldReturnResultOfWrappedMethod() {
		// Arrange
		byte[] expected = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

		BlueFileMetadata metadata = new BlueFileMetadata();
		metadata.put(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY, "valid-key");

		EncryptionService mockEncryptionService = mock(EncryptionService.class);
		when(mockEncryptionService.isEncryptionEnabled()).thenReturn(true);
		when(mockEncryptionService.getCurrentEncryptionVersionKey())
				.thenReturn("valid-key");
		when(mockEncryptionService.decrypt(anyString(), anyObject()))
				.thenReturn(expected);
		EncryptionServiceWrapper encryptionServiceWrapper = new EncryptionServiceWrapper(mockEncryptionService);

		// Act
		byte[] actual = encryptionServiceWrapper.decryptOrReturn(metadata, new byte[0]);

		// Assert
		assertArrayEquals(expected, actual);
	}

	@Test
	public void test_decryptOrReturn_unencryptedFile_shouldReturnUnchangedBytes() {
		// Arrange
		byte[] fakeDecryptedBytes = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
		byte[] expected = new byte[] {99, 98, 97};

		BlueFileMetadata metadata = new BlueFileMetadata();

		EncryptionService mockEncryptionService = mock(EncryptionService.class);
		when(mockEncryptionService.isEncryptionEnabled()).thenReturn(true);
		when(mockEncryptionService.getCurrentEncryptionVersionKey())
				.thenReturn("valid-key");
		when(mockEncryptionService.decrypt(anyString(), anyObject()))
				.thenReturn(fakeDecryptedBytes);
		EncryptionServiceWrapper encryptionServiceWrapper = new EncryptionServiceWrapper(mockEncryptionService);

		// Act
		byte[] actual = encryptionServiceWrapper.decryptOrReturn(metadata, expected);

		// Assert
		assertArrayEquals(expected, actual);
	}

	@Test
	public void test_decryptOrReturn_nullEncryptionService_shouldReturnUnchangedBytes() {
		// Arrange
		byte[] expected = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

		BlueFileMetadata metadata = new BlueFileMetadata();
		metadata.put(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY, "valid-key");

		EncryptionServiceWrapper encryptionServiceWrapper = new EncryptionServiceWrapper(null);

		// Act
		byte[] actual = encryptionServiceWrapper.decryptOrReturn(metadata, expected);

		// Assert
		assertArrayEquals(expected, actual);
	}

}
