package org.bluedb.disk.encryption;

import java.util.Optional;
import org.bluedb.disk.metadata.BlueFileMetadata;

/**
 * Wrapper class for interacting with an {@code EncryptionService} implementation optionally supplied on creation of a BlueDB instance.
 */
public class EncryptionServiceWrapper {

	/**
	 * An implementation of {@code EncryptionService} supplied on creation of a BlueDB instance, or null if none was provided.
	 */
	protected final EncryptionService encryptionService;

	/**
	 * A cached encryption version key populated when an instance of this class is created.
	 * Used to detect changes to the wrapped {@code EncryptionService}, skipping unneeded validation of this field.
	 */
	protected String cachedEncryptionVersionKey;

	public EncryptionServiceWrapper(EncryptionService encryptionService) {
		this.encryptionService = encryptionService;
		if (this.encryptionService != null) {
			this.cachedEncryptionVersionKey = this.getCurrentEncryptionVersionKey();
		}
	}

	/**
	 * Returns whether or not encryption is enabled.
	 *
	 * @return true if an encryption service has been supplied and encryption is enabled, false otherwise.
	 */
	public boolean isEncryptionEnabled() {
		return encryptionService != null && encryptionService.isEncryptionEnabled();
	}

	/**
	 * Returns the current encryption version key or the most recent valid key if the current one is invalid, logging warnings for any validation errors.
	 *
	 * @return the current encryption version key or the cached key if the current one is invalid.
	 * @throws IllegalStateException if the current encryption version key is invalid and no cached key exists.
	 */
	public String getCurrentEncryptionVersionKey() {

		if (!isEncryptionEnabled()) {
			return null;
		}
		String currentKey = encryptionService.getCurrentEncryptionVersionKey();

		// Return without validation if key has not changed
		if (cachedEncryptionVersionKey != null && cachedEncryptionVersionKey.equals(currentKey)) {
			return cachedEncryptionVersionKey; // Key has not changed
		}

		// Return new key if valid
		if (EncryptionUtils.isValidEncryptionVersionKey(currentKey)) {
			cachedEncryptionVersionKey = currentKey;
			return currentKey;
		}

		// Return cached key if one exists
		if (cachedEncryptionVersionKey != null) {
			System.out.println("[BlueDb Warning] - current encryption version key is invalid, this should be fixed ASAP! Using most recent valid key.");
			return cachedEncryptionVersionKey;
		}
		// Error only if current key is invalid and no cached key exists
		throw new IllegalStateException("getCurrentEncryptionVersionKey cannot be null or whitespace and must be no longer than " + EncryptionUtils.ENCRYPTION_VERSION_KEY_MAX_LENGTH + " characters.");
	}

	/**
	 * Encrypts and returns the bytes using a given encryption version key.
	 *
	 * @param encryptionVersionKey the encryption version key to use for encryption.
	 * @param bytes                the bytes to encrypt.
	 * @return the encrypted bytes or the unmodified bytes if encryption is not enabled.
	 */
	public byte[] encryptOrThrow(String encryptionVersionKey, byte[] bytes) {
		if (this.encryptionService != null) {
			return this.encrypt(encryptionVersionKey, bytes);
		}
		throw new IllegalStateException("Unable to encrypt, encryption service not supplied");
	}

	/**
	 * Decrypts and returns the bytes if encryption is enabled and the {@code BlueFileMetadata} supplied indicates the file is encrypted. Uses the encryption version key from the file extension to decrypt.
	 *
	 * @param fileMetadata the {@link BlueFileMetadata} retrieved from the file with information regarding the encryption status of the file.
	 * @param bytes        the bytes to decrypt.
	 * @return the decrypted bytes or the unmodified bytes.
	 */
	public byte[] decryptOrReturn(BlueFileMetadata fileMetadata, byte[] bytes) {
		if (encryptionService != null) {
			Optional<String> encryptionVersionKey = EncryptionUtils.getEncryptionVersionKey(fileMetadata);
			if (encryptionVersionKey.isPresent()) {
				return decrypt(encryptionVersionKey.get(), bytes);
			}
		}
		return bytes;
	}

	private byte[] encrypt(String encryptionVersionKey, byte[] bytes) {
		return encryptionService.encrypt(encryptionVersionKey, bytes);
	}

	private byte[] decrypt(String encryptionVersionKey, byte[] bytes) {
		return encryptionService.decrypt(encryptionVersionKey, bytes);
	}

}
