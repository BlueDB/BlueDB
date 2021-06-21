package org.bluedb.api.encryption;

public interface EncryptionConfig {

	String getCurrentEncryptionKeyVersion();
	byte[] decrypt(String encryptionKeyVersion, byte[] bytesToDecrypt);
	byte[] encrypt(String encryptionKeyVersion, byte[] bytesToEncrypt);

}
