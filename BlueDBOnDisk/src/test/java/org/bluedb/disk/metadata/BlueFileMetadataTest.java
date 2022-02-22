package org.bluedb.disk.metadata;

import org.junit.Test;
import static org.junit.Assert.*;

public class BlueFileMetadataTest {
	
	@Test
	public void test_putContainsGetRemove() {
		BlueFileMetadata metadata = new BlueFileMetadata();
		assertNull(metadata.get(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY).orElse(null));
		assertFalse(metadata.containsKey(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY));
		
		metadata.put(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY, "v1");
		assertTrue(metadata.containsKey(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY));
		assertEquals("v1", metadata.get(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY).get());
		
		metadata.put(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY, "v2");
		assertTrue(metadata.containsKey(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY));
		assertEquals("v2", metadata.get(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY).get());
		
		assertEquals("v2", metadata.remove(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY));
		assertNull(metadata.get(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY).orElse(null));
		assertFalse(metadata.containsKey(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY));
	}
	
	@Test
	public void test_isTrue() {
		BlueFileMetadata metadata = new BlueFileMetadata();
		assertFalse(metadata.isTrue(BlueFileMetadataKey.SORTED_MASS_CHANGE_FILE));
		
		metadata.put(BlueFileMetadataKey.SORTED_MASS_CHANGE_FILE, "something random");
		assertFalse(metadata.isTrue(BlueFileMetadataKey.SORTED_MASS_CHANGE_FILE));
		
		metadata.put(BlueFileMetadataKey.SORTED_MASS_CHANGE_FILE, "True");
		assertTrue(metadata.isTrue(BlueFileMetadataKey.SORTED_MASS_CHANGE_FILE));
		
		metadata.put(BlueFileMetadataKey.SORTED_MASS_CHANGE_FILE, "true");
		assertTrue(metadata.isTrue(BlueFileMetadataKey.SORTED_MASS_CHANGE_FILE));
		
		metadata.put(BlueFileMetadataKey.SORTED_MASS_CHANGE_FILE, "False");
		assertFalse(metadata.isTrue(BlueFileMetadataKey.SORTED_MASS_CHANGE_FILE));
		
		metadata.put(BlueFileMetadataKey.SORTED_MASS_CHANGE_FILE, "false");
		assertFalse(metadata.isTrue(BlueFileMetadataKey.SORTED_MASS_CHANGE_FILE));
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void test_put_nullKey_ThrowsIllegalArgumentException() {
		// Arrange
		BlueFileMetadata metadata = new BlueFileMetadata();
		// Act
		metadata.put(null, "test");
		// No assert needed, handled by annotation
	}

	@Test(expected = IllegalArgumentException.class)
	public void test_put_nullValue_ThrowsIllegalArgumentException() {
		// Arrange
		BlueFileMetadata metadata = new BlueFileMetadata();
		// Act
		metadata.put(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY, null);
		// No assert needed, handled by annotation
	}

	@Test(expected = IllegalArgumentException.class)
	public void test_remove_nullKey_ThrowsIllegalArgumentException() {
		// Arrange
		BlueFileMetadata metadata = new BlueFileMetadata();
		// Act
		metadata.remove(null);
		// No assert needed, handled by annotation
	}

	@Test
	public void test_remove_unusedKey_ReturnsEmpty() {
		// Arrange
		BlueFileMetadata metadata = new BlueFileMetadata();
		// Act
		String actual = metadata.remove(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY);
		// Assert
		assertNull(actual);
	}
}
