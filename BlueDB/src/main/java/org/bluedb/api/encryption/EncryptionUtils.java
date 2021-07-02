package org.bluedb.api.encryption;

import java.util.Optional;
import org.bluedb.api.metadata.BlueFileMetadata;
import org.bluedb.api.metadata.BlueFileMetadataKey;

/**
 * Static utility methods and fields that assist with the encryption and decryption processes.
 */
public class EncryptionUtils {

	/**
	 * The max number of characters allowed in an encryption version key.
	 */
	public static final int ENCRYPTION_VERSION_KEY_MAX_LENGTH = 5;

	/**
	 * Get the encryption version key for a file with a given {@code BlueFileMetadata}.
	 *
	 * @param fileMetadata the {@link BlueFileMetadata} of the file.
	 * @return the encryption version key of the given file or an empty optional if the file is not encrypted.
	 */
	public static Optional<String> getEncryptionVersionKey(BlueFileMetadata fileMetadata) {
		if (fileMetadata == null || !fileMetadata.containsKey(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY)) {
			return Optional.empty();
		}
		return Optional.ofNullable(String.valueOf(fileMetadata.get(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY)));

	}

	/**
	 * Returns a boolean representing whether the given {@code String} is a valid encryption version key.
	 *
	 * @param key the {@code String} to validate
	 * @return true if the key is valid, false if invalid.
	 */
	public static boolean isValidEncryptionVersionKey(String key) {
		return key != null && key.length() <= EncryptionUtils.ENCRYPTION_VERSION_KEY_MAX_LENGTH;
	}

	public static boolean shouldEncrypt(BlueFileMetadata oldFileMetadata, BlueFileMetadata newFileMetadata) {
		String oldEncryptionVersionKey = (String) oldFileMetadata.get(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY);
		String newEncryptionVersionKey = (String) newFileMetadata.get(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY);
		
		return newEncryptionVersionKey != null;
	}

}
