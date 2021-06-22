package org.bluedb.api.encryption;

public interface EncryptionService {

	boolean isEncryptionEnabled();
	String getCurrentEncryptionVersionKey();
	byte[] decrypt(String encryptionVersionKey, byte[] bytesToDecrypt);
	byte[] encrypt(String encryptionVersionKey, byte[] bytesToEncrypt);

}
