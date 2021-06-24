package org.bluedb.api.encryption;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Static utility methods and fields that assist with the encryption and decryption processes.
 */
public class EncryptionUtils {

	/**
	 * The base file extension of an encrypted BlueDB file without the version key.
	 */
	public static final String ENCRYPTED_FILE_BASE_EXTENSION = ".ebf";

	/**
	 * The max number of characters allowed in an encryption version key.
	 */
	public static final int ENCRYPTION_VERSION_KEY_MAX_LENGTH = 5;

	/**
	 * Get the encryption version key for a given Path if the file is a {@value #ENCRYPTED_FILE_BASE_EXTENSION} file.
	 *
	 * @param path the file path to check for an encryption version key.
	 * @return the encryption version key of the given file or an empty optional if the file is not encrypted.
	 */
	public static Optional<String> getEncryptionVersionKey(String path) {
		return Optional.ofNullable(path)
				.filter(f -> f.contains(ENCRYPTED_FILE_BASE_EXTENSION + "."))
				.map(f -> f.substring(path.lastIndexOf(".") + 1));
	}

	/**
	 * Get the encryption version key for a given Path if the file is a {@value #ENCRYPTED_FILE_BASE_EXTENSION} file.
	 *
	 * @param path the file path to check for an encryption version key.
	 * @return the encryption version key of the given file or an empty optional if the file is not encrypted.
	 */
	public static Optional<String> getEncryptionVersionKey(Path path) {
		return getEncryptionVersionKey(path.getFileName().toString());
	}

	/**
	 * Returns a boolean representing whether the given {@code String} is a valid encryption version key.
	 * @param key the {@code String} to validate
	 * @return true if the key is valid, false if invalid.
	 */
	public static boolean isValidEncryptionVersionKey(String key) {
		boolean isValidEncryptionVersionKey = false;
		if (key != null && key.length() <= EncryptionUtils.ENCRYPTION_VERSION_KEY_MAX_LENGTH) {
			try {
				File.createTempFile("tmp", EncryptionUtils.ENCRYPTED_FILE_BASE_EXTENSION + "." + key); // IOException if characters are invalid for OS
				isValidEncryptionVersionKey = true;
			} catch (IOException e) {
				isValidEncryptionVersionKey = false;
			}
		}
		return isValidEncryptionVersionKey;
	}

}
