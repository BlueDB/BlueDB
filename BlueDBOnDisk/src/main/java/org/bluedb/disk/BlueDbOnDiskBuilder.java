package org.bluedb.disk;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.bluedb.api.BlueDb;
import org.bluedb.api.ReadableBlueDb;
import org.bluedb.disk.encryption.EncryptionService;
import org.bluedb.disk.encryption.EncryptionUtils;

/**
 * A builder for the {@link ReadableDbOnDisk} and {@link ReadWriteDbOnDisk} classes
 */
public class BlueDbOnDiskBuilder {
	private Path path = Paths.get(".", "bluedb");
	private EncryptionService encryptionService = null;

	/**
	 * Sets the path you wish to use for the BlueDB data
	 * @param path the path directory that will contain the BlueDB data
	 * @return itself with the path set
	 */
	public BlueDbOnDiskBuilder withPath(Path path) {
		this.path = path;
		return this;
	}

	/**
	 * Sets the {@link EncryptionService} you wish to use for your BlueDB instance
	 *
	 * @param encryptionService the encryption service specifying how to encrypt and decrypt data
	 * @return itself with the encryption service set
	 */
	public BlueDbOnDiskBuilder withEncryptionService(EncryptionService encryptionService) {
		if (this.encryptionService != null) {
			throw new IllegalStateException("encryption can only be enabled once");
		}
		if (encryptionService == null) {
			throw new IllegalArgumentException("encryptionService cannot be null");
		}
		if (!EncryptionUtils.isValidEncryptionVersionKey(encryptionService.getCurrentEncryptionVersionKey())) {
			throw new IllegalArgumentException("value returned from encryptionService#getCurrentEncryptionVersionKey() cannot be null or whitespace, and must be no longer than " + EncryptionUtils.ENCRYPTION_VERSION_KEY_MAX_LENGTH + " characters");
		}

		this.encryptionService = encryptionService;
		return this;
	}

	/**
	 * Builds the {@link BlueDb} object
	 * @return the {@link BlueDb} built
	 */
	public BlueDb build() {
		return new ReadWriteDbOnDisk(path, encryptionService);
	}

	/**
	 * Builds the {@link ReadableBlueDb} object
	 * @return the {@link ReadableBlueDb} built
	 */
	public ReadableBlueDb buildReadOnly() {
		return new ReadableDbOnDisk(path, encryptionService);
	}
	
	/**
	 * Replaced by withPath<br><br>
	 * Sets the path you wish to use for the BlueDB data
	 * @param path the path directory that will contain the BlueDB data
	 * @return itself with the path set
	 */
	@Deprecated
	public BlueDbOnDiskBuilder setPath(Path path) {
		this.path = path;
		return this;
	}
}
