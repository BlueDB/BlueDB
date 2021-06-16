package org.bluedb.api.encryption;

public interface ReadableBlueDbEncryptionConfig {

	String getCurrentEncryptionKeyVersion();
	byte[] decrypt(String encryptionKeyVersion, byte[] bytesToDecrypt);

}
