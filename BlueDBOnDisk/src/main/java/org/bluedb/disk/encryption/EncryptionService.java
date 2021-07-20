package org.bluedb.disk.encryption;

public interface EncryptionService {

	/**
	 * Returns whether or not encryption operations should be performed. This 
	 *
	 * @return true if an encryption service has been supplied and encryption is enabled, false otherwise.
	 */
	boolean isEncryptionEnabled();
	String getCurrentEncryptionVersionKey();
	byte[] decrypt(String encryptionVersionKey, byte[] bytesToDecrypt);
	byte[] encrypt(String encryptionVersionKey, byte[] bytesToEncrypt);

}
