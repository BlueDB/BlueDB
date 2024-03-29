package org.bluedb.disk.encryption;

import java.util.Objects;
import java.util.Optional;
import org.bluedb.disk.metadata.BlueFileMetadata;
import org.bluedb.disk.metadata.BlueFileMetadataKey;

/**
 * Static utility methods and fields that assist with the encryption and decryption processes.
 */
public class EncryptionUtils {

	/**
	 * The max number of characters allowed in an encryption version key.
	 */
	public static final int ENCRYPTION_VERSION_KEY_MAX_LENGTH = 20;

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
		return fileMetadata.get(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY);

	}

	/**
	 * Returns a boolean representing whether the given {@code String} is a valid encryption version key.
	 *
	 * @param key the {@code String} to validate
	 * @return true if the key is valid, false if invalid.
	 */
	public static boolean isValidEncryptionVersionKey(String key) {
		return key != null && !key.trim().isEmpty() && key.length() <= EncryptionUtils.ENCRYPTION_VERSION_KEY_MAX_LENGTH;
	}

	/**
	 * Method to be used by a {@code StreamingWriter} or other classes handling file writes. Determines if encryption can forcefully be skipped for unchanged bytes, instead writing the raw bytes from the original file.
	 * Prevents unnecessary encryption of unencrypted files or files that have not had their encryption version changed.
	 *
	 * @param oldFileMetadata the {@code BlueFileMetadata} of the original file being rewritten for an atomic swap.
	 * @param newFileMetadata the {@code BlueFileMetadata} of the new file being written.
	 * @return true if neither file is encrypted or the encryption version has not changed, false otherwise.
	 */
	public static boolean shouldWriterSkipEncryptionForUnchangedDataUsingRawBytes(BlueFileMetadata oldFileMetadata, BlueFileMetadata newFileMetadata) {
		Optional<String> oldEncryptionVersionKey = oldFileMetadata == null ? Optional.empty() : oldFileMetadata.get(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY);
		Optional<String> newEncryptionVersionKey = newFileMetadata == null ? Optional.empty() : newFileMetadata.get(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY);
		return Objects.equals(oldEncryptionVersionKey, newEncryptionVersionKey);
	}

}
