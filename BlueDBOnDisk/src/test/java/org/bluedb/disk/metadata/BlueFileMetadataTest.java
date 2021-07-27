package org.bluedb.disk.metadata;

import org.junit.Test;

public class BlueFileMetadataTest {
	
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
}
