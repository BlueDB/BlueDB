package org.bluedb.api.encryption;

import java.util.Optional;
import org.bluedb.api.metadata.BlueFileMetadata;

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
	 * Encrypts and returns the bytes if encryption is enabled. Uses {@link EncryptionServiceWrapper#getCurrentEncryptionVersionKey()} to determine which encryption version to use.
	 *
	 * @param bytes the bytes to encrypt.
	 * @return the encrypted bytes or the unmodified bytes if encryption is not enabled.
	 */
	public byte[] encryptOrReturn(byte[] bytes) {
		if (isEncryptionEnabled()) {
			return encrypt(this.getCurrentEncryptionVersionKey(), bytes);
		}
		return bytes;
	}

	/**
	 * Encrypts and returns the bytes using a given encryption version key.
	 *
	 * @param encryptionVersionKey the encryption version key to use for encryption.
	 * @param bytes the bytes to encrypt.
	 * @return the encrypted bytes or the unmodified bytes if encryption is not enabled.
	 */
	public byte[] encryptOrThrow(String encryptionVersionKey, byte[] bytes) {
		if (this.encryptionService != null) {
			return this.encrypt(encryptionVersionKey, bytes);
		}
		throw new IllegalStateException("Unable to encrypt, encryption service not supplied");
	}

	/**
	 * Decrypts and returns the bytes if encryption is enabled.
	 *
	 * @param encryptionVersionKey the encryption version key to use.
	 * @param bytes                the bytes to decrypt.
	 * @return the decrypted bytes or the unmodified bytes if encryption is not enabled.
	 */
	public byte[] decryptOrReturn(String encryptionVersionKey, byte[] bytes) {
		if (isEncryptionEnabled()) {
			return decrypt(encryptionVersionKey, bytes);
		}
		return bytes;
	}

	/**
	 * Decrypts and returns the bytes if encryption is enabled and the {@code BlueFileMetadata} supplied indicates the file is encrypted. Uses the encryption version key from the file extension to decrypt.
	 *
	 * @param fileMetadata the {@link BlueFileMetadata} retrieved from the file with information regarding the encryption status of the file.
	 * @param bytes    the bytes to decrypt.
	 * @return the decrypted bytes or the unmodified bytes.
	 */
	public byte[] decryptOrReturn(BlueFileMetadata fileMetadata, byte[] bytes) {
		Optional<String> encryptionVersionKey;
		if (isEncryptionEnabled() && (encryptionVersionKey = EncryptionUtils.getEncryptionVersionKey(fileMetadata)).isPresent()) {
			return decrypt(encryptionVersionKey.get(), bytes);
		}
		return bytes;
	}

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
		if(!isEncryptionEnabled()) {
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
			// TODO Warning or error level log here
			return cachedEncryptionVersionKey;
		}

		// Error only if current key is invalid and no cached key exists
		throw new IllegalStateException("getCurrentEncryptionVersionKey must be no longer than " + EncryptionUtils.ENCRYPTION_VERSION_KEY_MAX_LENGTH + " characters and must be a valid file extension for your OS");
	}

	private byte[] encrypt(String encryptionVersionKey, byte[] bytes) {
		return encryptionService.encrypt(encryptionVersionKey, bytes);
	}
	
	private byte[] decrypt(String encryptionVersionKey, byte[] bytes) {
		return encryptionService.decrypt(encryptionVersionKey, bytes);
	}

}
