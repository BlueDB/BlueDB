package org.bluedb.disk.helpers;

import org.bluedb.disk.encryption.EncryptionService;
import org.jasypt.util.binary.BasicBinaryEncryptor;

public class SimpleEncryptionService implements EncryptionService {
	
	private String currentKey = "v1";
	private boolean encryptionEnabled = true;	

	@Override
	public boolean isEncryptionEnabled() {
		return this.encryptionEnabled;
	}

	@Override
	public String getCurrentEncryptionVersionKey() {
		return this.getCurrentKey();
	}

	@Override
	public byte[] encrypt(String encryptionVersionKey, byte[] bytesToEncrypt) {
		BasicBinaryEncryptor encryptor = new BasicBinaryEncryptor();
		encryptor.setPassword(encryptionVersionKey);
		return encryptor.encrypt(bytesToEncrypt);
	}

	@Override
	public byte[] decrypt(String encryptionVersionKey, byte[] bytesToDecrypt) {
		BasicBinaryEncryptor encryptor = new BasicBinaryEncryptor();
		encryptor.setPassword(encryptionVersionKey);
		return encryptor.decrypt(bytesToDecrypt);
	}

	public String getCurrentKey() {
		return currentKey;
	}

	public void setCurrentKey(String currentKey) {
		this.currentKey = currentKey;
	}

	public void setEncryptionEnabled(boolean encryptionEnabled) {
		this.encryptionEnabled = encryptionEnabled;
	}

}
