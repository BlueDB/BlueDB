package org.bluedb.api.encryption;

public interface ReadWriteBlueDbEncryptionConfig extends ReadableBlueDbEncryptionConfig {

	byte[] encrypt(String encryptionKeyVersion, byte[] bytesToEncrypt);

}
