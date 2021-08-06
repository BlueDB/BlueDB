package org.bluedb.disk.encryption;

public interface EncryptionService {

	/**
	 * Returns whether or not encryption operations should be performed on new writes.
	 *
	 * @return true if future writes should be encrypted, false otherwise.
	 */
	boolean isEncryptionEnabled();

	/**
	 * Gets the current encryption version key. The key acts as an identifier for the current encryption implementation.
	 * <p/>
	 * For a key to be valid, it cannot be null or whitespace, and it must be no longer than {@value EncryptionUtils#ENCRYPTION_VERSION_KEY_MAX_LENGTH} characters.
	 * <p/>
	 * Each time the encryption implementation is updated, a new encryption version key should be set.
	 * All encryption version keys used for a database should be handled in {@linkplain #encrypt(String, byte[])} and {@linkplain #decrypt(String, byte[])}.
	 *
	 * @return the current encryption version key.
	 */
	String getCurrentEncryptionVersionKey();

	/**
	 * Encrypt a byte array using a given encryption version key.
	 * <p/>
	 * This method should handle all encryption version keys used during the lifetime of a database.
	 * When the encryption or decryption implementation is changed, this method should still support encrypting using the old encryption version key.
	 * <p/>
	 * Example pattern to follow:
	 * <pre>{@code
	 * switch (encryptionVersionKey) {
	 *     case "v1":
	 *         // V1 ENCRYPTION IMPLEMENTATION HERE
	 *         break;
	 *     case "v2":
	 *         // V2 ENCRYPTION IMPLEMENTATION HERE
	 *         break;
	 *     ...
	 * }
	 * }</pre>
	 *
	 * @param encryptionVersionKey The encryption version key the bytes should encrypt with.
	 *                             <p/>
	 *                             Will be the return value of {@linkplain #getCurrentEncryptionVersionKey()} unless the value returned is invalid. Defaults to the most recent valid key cached in memory, if one exists.
	 * @param bytesToEncrypt       The bytes to encrypt.
	 * @return The encrypted bytes.
	 */
	byte[] encrypt(String encryptionVersionKey, byte[] bytesToEncrypt);

	/**
	 * Decrypt a byte array using a given encryption version key.
	 * <p/>
	 * This method should handle all encryption version keys used during the lifetime of a database.
	 * When the encryption or decryption implementation is changed, this method should still support decrypting using the old encryption version key.
	 * <p/>
	 * Example pattern to follow:
	 * <pre>{@code
	 * switch (encryptionVersionKey) {
	 *     case "v1":
	 *         // V1 DECRYPTION IMPLEMENTATION HERE
	 *         break;
	 *     case "v2":
	 *         // V2 DECRYPTION IMPLEMENTATION HERE
	 *         break;
	 *     ...
	 * }
	 * }</pre>
	 *
	 * @param encryptionVersionKey The encryption version key the bytes were encrypted with.
	 * @param bytesToDecrypt       The bytes to decrypt.
	 * @return The decrypted bytes.
	 */
	byte[] decrypt(String encryptionVersionKey, byte[] bytesToDecrypt);

}
