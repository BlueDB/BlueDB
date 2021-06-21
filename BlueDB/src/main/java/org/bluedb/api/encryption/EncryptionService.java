package org.bluedb.api.encryption;

import java.util.Optional;

public class EncryptionService {

	protected final Optional<EncryptionConfig> encryptionConfig;

	public EncryptionService(EncryptionConfig encryptionConfig) {
		if (encryptionConfig != null) {
			this.encryptionConfig = Optional.of(encryptionConfig);
		} else {
			this.encryptionConfig = Optional.empty();
		}
	}

	public byte[] encryptOrReturn(byte[] bytes) {
		if (encryptionConfig.isPresent()) {
			return encryptionConfig.get().encrypt(encryptionConfig.get().getCurrentEncryptionKeyVersion(), bytes);
		}
		return bytes;
	}

	public byte[] decryptOrReturn(byte[] bytes) {
		if (encryptionConfig.isPresent()) {
			return encryptionConfig.get().decrypt(encryptionConfig.get().getCurrentEncryptionKeyVersion(), bytes);
		}
		return bytes;
	}

}
