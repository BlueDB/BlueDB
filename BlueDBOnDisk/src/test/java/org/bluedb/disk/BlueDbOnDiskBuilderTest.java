package org.bluedb.disk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.bluedb.disk.config.ConfigurationService;
import org.bluedb.disk.encryption.EncryptionService;
import org.bluedb.disk.encryption.EncryptionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class BlueDbOnDiskBuilderTest {
	
	private Path tempDir;

	@Before
	public void before() throws IOException {
		tempDir = Files.createTempDirectory("BlueDbOnDiskBuilderTest");
		tempDir.toFile().deleteOnExit();
	}
	
	@After
	public void after() throws IOException {
		tempDir.toFile().delete();
	}

	@Test
	public void test_withEncryptionService_validEncryptionService_succeeds() {
		// Arrange
		EncryptionService mockEncryptionService = Mockito.mock(EncryptionService.class);
		when(mockEncryptionService.isEncryptionEnabled()).thenReturn(true);
		when(mockEncryptionService.getCurrentEncryptionVersionKey()).thenReturn("valid-key");
		// Act
		new BlueDbOnDiskBuilder().withEncryptionService(mockEncryptionService);
		// No assert needed, passes if no error is thrown
	}

	@Test
	public void test_withEncryptionService_validEncryptionServiceEnabledTwice_throwsIllegalStateException() {
		// Arrange
		String expected = "encryption can only be enabled once";
		EncryptionService mockEncryptionService = Mockito.mock(EncryptionService.class);
		when(mockEncryptionService.isEncryptionEnabled()).thenReturn(true);
		when(mockEncryptionService.getCurrentEncryptionVersionKey()).thenReturn("valid-key");
		// Act
		try {
			new BlueDbOnDiskBuilder()
					.withEncryptionService(mockEncryptionService)
					.withEncryptionService(mockEncryptionService);
			fail("Expected exception was not thrown");
		} catch (IllegalStateException ex) {
			// Assert
			assertEquals(expected, ex.getMessage());
		}
	}

	@Test
	public void test_withEncryptionService_invalidEncryptionVersionKey_throwsIllegalArgumentException() {
		// Arrange
		String expected = "value returned from encryptionService#getCurrentEncryptionVersionKey() cannot be null or whitespace, and must be no longer than " + EncryptionUtils.ENCRYPTION_VERSION_KEY_MAX_LENGTH + " characters";
		EncryptionService mockEncryptionService = Mockito.mock(EncryptionService.class);
		when(mockEncryptionService.isEncryptionEnabled()).thenReturn(true);
		when(mockEncryptionService.getCurrentEncryptionVersionKey()).thenReturn(null);
		// Act
		try {
			new BlueDbOnDiskBuilder().withEncryptionService(mockEncryptionService);
			fail("Expected exception was not thrown");
		} catch (IllegalArgumentException ex) {
			// Assert
			assertEquals(expected, ex.getMessage());
		}
	}

	@Test
	public void test_withEncryptionService_nullEncryptionService_throwsIllegalArgumentException() {
		// Arrange
		String expected = "encryptionService cannot be null";
		// Act
		try {
			new BlueDbOnDiskBuilder().withEncryptionService(null);
			fail("Expected exception was not thrown");
		} catch (IllegalArgumentException ex) {
			// Assert
			assertEquals(expected, ex.getMessage());
		}
	}
	
	@Test
	public void test_withConfigurationService_nullThrowsException() {
		try {
			new BlueDbOnDiskBuilder()
				.withPath(tempDir.resolve("test-collection"))
				.withConfigurationService(null)
				.build();
			fail("Expected exception was not thrown");
		} catch (IllegalArgumentException ex) {
			//Expected
		}
	}
	
	@Test
	public void test_withConfigurationService_setterWorks() {
		ConfigurationService mockedConfigurationService = Mockito.mock(ConfigurationService.class);
		Mockito.doReturn(false).when(mockedConfigurationService).shouldValidateObjects();
		
		ReadWriteDbOnDisk db = (ReadWriteDbOnDisk) new BlueDbOnDiskBuilder()
			.withPath(tempDir.resolve("test-collection"))
			.withConfigurationService(mockedConfigurationService)
			.build();
		
		assertFalse(db.getConfigurationService().shouldValidateObjects());
		
		Mockito.doReturn(true).when(mockedConfigurationService).shouldValidateObjects();
		db = (ReadWriteDbOnDisk) new BlueDbOnDiskBuilder()
				.withPath(tempDir.resolve("test-collection"))
				.withConfigurationService(mockedConfigurationService)
				.build();
		assertTrue(db.getConfigurationService().shouldValidateObjects());
	}

}
