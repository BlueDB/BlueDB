package org.bluedb.api.encryption;

import java.util.Optional;

public class EncryptionServiceWrapper {

	protected final Optional<EncryptionService> encryptionService;

	public EncryptionServiceWrapper(EncryptionService encryptionService) {
		this.encryptionService = Optional.ofNullable(encryptionService);
	}

	public byte[] encryptOrReturn(String encryptionVersionKey, byte[] bytes) {
		if (isEncryptionEnabled()) {
			return encryptionService.get().encrypt(encryptionVersionKey, bytes);
		}
		return bytes;
	}

	public byte[] decryptOrReturn(String encryptionVersionKey, byte[] bytes) {
		if (isEncryptionEnabled()) {
			return encryptionService.get().decrypt(encryptionVersionKey, bytes);
		}
		return bytes;
	}

	public String getCurrentEncryptionVersionKey() {
		if (isEncryptionEnabled()) {
			// TODO add validation of this field here
			return encryptionService.get().getCurrentEncryptionVersionKey();
		}
		return null;
	}

	private boolean isEncryptionEnabled() {
		return encryptionService.isPresent() && encryptionService.get().isEncryptionEnabled();
	}

}
