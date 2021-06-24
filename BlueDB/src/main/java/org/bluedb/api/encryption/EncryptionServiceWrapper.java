package org.bluedb.api.encryption;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Wrapper class for interacting with an {@code EncryptionService} implementation optionally supplied on creation of a BlueDB instance.
 */
public class EncryptionServiceWrapper {

	/**
	 * An implementation of {@code EncryptionService} supplied on creation of a BlueDB instance, or an empty {@code Optional} if none was provided.
	 */
	protected final Optional<EncryptionService> encryptionService;

	/**
	 * A cached encryption version key populated when an instance of this class is created.
	 * Used to detect changes to the wrapped {@code EncryptionService}, skipping unneeded validation of this field.
	 */
	protected String cachedEncryptionVersionKey;

	public EncryptionServiceWrapper(EncryptionService encryptionService) {
		this.encryptionService = Optional.ofNullable(encryptionService);
		if (this.encryptionService.isPresent()) {
			this.cachedEncryptionVersionKey = this.getMostRecentValidEncryptionVersionKey();
		}
	}

	/**
	 * Encrypts and returns the bytes if encryption is enabled. Uses {@link EncryptionServiceWrapper#getMostRecentValidEncryptionVersionKey()} to determine which encryption version to use.
	 *
	 * @param bytes the bytes to encrypt.
	 * @return the encrypted bytes or the unmodified bytes if encryption is not enabled.
	 */
	public byte[] encryptOrReturn(byte[] bytes) {
		if (isEncryptionEnabled()) {
			return encrypt(this.getMostRecentValidEncryptionVersionKey(), bytes);
		}
		return bytes;
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
	 * Decrypts and returns the bytes if encryption is enabled and the {@code Path} supplied is an encrypted file. Uses the encryption version key from the file extension to decrypt.
	 *
	 * @param filePath the {@code Path} the bytes are being decrypted from.
	 * @param bytes    the bytes to decrypt.
	 * @return the decrypted bytes or the unmodified bytes.
	 */
	public byte[] decryptOrReturn(Path filePath, byte[] bytes) {
		Optional<String> encryptionVersionKey;
		if (isEncryptionEnabled() && (encryptionVersionKey = EncryptionUtils.getEncryptionVersionKey(filePath)).isPresent()) {
			return decrypt(encryptionVersionKey.get(), bytes);
		}
		return bytes;
	}

	/**
	 * If encryption is enabled, returns the given {@code Path} with the encryption extension and the current encryption version key appended to it.
	 *
	 * @param filePath the {@code Path} to append the encryption extension to.
	 * @return the updated {@code Path} if encryption is enabled or the unmodified {@code Path}.
	 */
	public Path addEncryptionExtensionOrReturn(Path filePath) {
		if (isEncryptionEnabled()) {
			return filePath.resolveSibling(filePath.getFileName() + EncryptionUtils.ENCRYPTED_FILE_BASE_EXTENSION + "." + getMostRecentValidEncryptionVersionKey());
		}
		return filePath;
	}

	/**
	 * Returns the current encryption version key or the most recent valid key if the current one is invalid, logging warnings for any validation errors.
	 *
	 * @return the current encryption version key or the cached key if the current one is invalid.
	 * @throws IllegalStateException if the current encryption version key is invalid and no cached key exists.
	 */
	private String getMostRecentValidEncryptionVersionKey() {
		String currentKey = encryptionService.get().getCurrentEncryptionVersionKey();

		// Return without validation if key has not changed
		if (cachedEncryptionVersionKey != null && cachedEncryptionVersionKey.equals(currentKey)) {
			return cachedEncryptionVersionKey; // Key has not changed
		}

		// Validate new key
		boolean validEncryptionVersionKey = false;
		if (currentKey != null && currentKey.length() <= EncryptionUtils.ENCRYPTION_VERSION_KEY_MAX_LENGTH) {
			try {
				File.createTempFile("tmp", EncryptionUtils.ENCRYPTED_FILE_BASE_EXTENSION + "." + currentKey); // IOException if characters are invalid for OS
				validEncryptionVersionKey = true;
			} catch (IOException e) {
				validEncryptionVersionKey = false;
			}
		}
		if (validEncryptionVersionKey) {
			cachedEncryptionVersionKey = currentKey;
			return currentKey;
		}

		// Return cached key if current key is invalid
		if (cachedEncryptionVersionKey != null) {
			// TODO Warning or error level log here
			return cachedEncryptionVersionKey;
		}

		// Error if no cached key and current key is invalid
		throw new IllegalStateException("getCurrentEncryptionVersionKey must be no longer than " + EncryptionUtils.ENCRYPTION_VERSION_KEY_MAX_LENGTH + " characters and must be a valid file extension for your OS");
	}

	private byte[] encrypt(String encryptionVersionKey, byte[] bytes) {
		return encryptionService.get().decrypt(encryptionVersionKey, bytes);
	}

	private byte[] decrypt(String encryptionVersionKey, byte[] bytes) {
		return encryptionService.get().decrypt(encryptionVersionKey, bytes);
	}

	private boolean isEncryptionEnabled() {
		return encryptionService.isPresent() && encryptionService.get().isEncryptionEnabled();
	}

}
