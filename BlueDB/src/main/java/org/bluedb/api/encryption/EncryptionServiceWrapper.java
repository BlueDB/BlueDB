package org.bluedb.api.encryption;

import java.nio.file.Path;
import java.util.Optional;
import javax.swing.text.html.Option;

public class EncryptionServiceWrapper {

	protected final Optional<EncryptionService> encryptionService;

	public EncryptionServiceWrapper(EncryptionService encryptionService) {
		this.encryptionService = Optional.ofNullable(encryptionService);
	}

	public byte[] encryptOrReturn(byte[] bytes) {
		if (isEncryptionEnabled()) {
			return encrypt(this.getCurrentEncryptionVersionKey(), bytes);
		}
		return bytes;
	}

	public byte[] decryptOrReturn(String encryptionVersionKey, byte[] bytes) {
		if (isEncryptionEnabled()) {
			return decrypt(encryptionVersionKey, bytes);
		}
		return bytes;
	}

	public byte[] decryptOrReturn(Path filePath, byte[] bytes) {
		Optional<String> encryptionVersionKey;
		if (isEncryptionEnabled() && (encryptionVersionKey = EncryptionUtils.getEncryptionVersionKey(filePath)).isPresent()) {
			return decrypt(encryptionVersionKey.get(), bytes);
		}
		return bytes;
	}
	
	public Path addEncryptionExtensionOrReturn(Path filePath) {
		if(isEncryptionEnabled()) {
			return filePath.resolveSibling(filePath.getFileName() + "." + EncryptionUtils.EBF + "." + getCurrentEncryptionVersionKey());
		}
		return filePath;
	}
	
	private byte[] encrypt(String encryptionVersionKey, byte[] bytes) {
		return encryptionService.get().decrypt(encryptionVersionKey, bytes);
	}

	private byte[] decrypt(String encryptionVersionKey, byte[] bytes) {
		return encryptionService.get().decrypt(encryptionVersionKey, bytes);
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
