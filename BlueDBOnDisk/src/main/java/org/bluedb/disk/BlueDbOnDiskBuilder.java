package org.bluedb.disk;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.bluedb.api.BlueDb;
import org.bluedb.api.ReadableBlueDb;
import org.bluedb.api.encryption.ReadWriteBlueDbEncryptionConfig;
import org.bluedb.api.encryption.ReadableBlueDbEncryptionConfig;

/**
 * A builder for the {@link ReadableDbOnDisk} and {@link ReadWriteDbOnDisk} classes
 */
public class BlueDbOnDiskBuilder {

	private Path path = Paths.get(".", "bluedb");
	private ReadableBlueDbEncryptionConfig readableBlueDbEncryptionConfig = null;
	private ReadWriteBlueDbEncryptionConfig readWriteBlueDbEncryptionConfig = null;

	/**
	 * Sets the path you wish to use for the BlueDB data
	 *
	 * @param path the path directory that will contain the BlueDB data
	 * @return itself with the path set
	 */
	public BlueDbOnDiskBuilder withPath(Path path) {
		this.path = path;
		return this;
	}

	/**
	 * Enables encryption with a given encryption config.
	 *
	 * @param encryptionConfig the encryption config specifying how to encrypt and decrypt data
	 * @return itself with the encryption config set
	 */
	public BlueDbOnDiskBuilder enableEncryption(ReadWriteBlueDbEncryptionConfig encryptionConfig) {
		if (this.readableBlueDbEncryptionConfig != null || this.readWriteBlueDbEncryptionConfig != null) {
			throw new IllegalStateException("encryption can only be enabled once");
		}
		if (encryptionConfig == null) {
			throw new IllegalArgumentException("encryptionConfig cannot be null");
		}
		if (encryptionConfig.getCurrentEncryptionKeyVersion() == null || encryptionConfig.getCurrentEncryptionKeyVersion().trim().isEmpty()) {
			throw new IllegalArgumentException("encryptionConfig#getCurrentEncryptionKeyVersion() cannot be null or empty");
		}

		this.readWriteBlueDbEncryptionConfig = encryptionConfig;
		return this;
	}

	/**
	 * Enables encryption with a given encryption config. Should only be used when building a {@link ReadableDbOnDisk}. When building a {@link ReadWriteDbOnDisk}, use {@link BlueDbOnDiskBuilder#enableEncryption(ReadWriteBlueDbEncryptionConfig)}
	 *
	 * @param encryptionConfig the encryption config specifying how to decrypt data
	 * @return itself with the encryption config set
	 */
	public BlueDbOnDiskBuilder enableEncryption(ReadableBlueDbEncryptionConfig encryptionConfig) {
		if (this.readableBlueDbEncryptionConfig != null || this.readWriteBlueDbEncryptionConfig != null) {
			throw new IllegalStateException("encryption can only be enabled once");
		}
		if (encryptionConfig == null) {
			throw new IllegalArgumentException("encryptionConfig cannot be null");
		}
		if (encryptionConfig.getCurrentEncryptionKeyVersion() == null || encryptionConfig.getCurrentEncryptionKeyVersion().trim().isEmpty()) {
			throw new IllegalArgumentException("encryptionConfig#getCurrentEncryptionKeyVersion() cannot be null or empty");
		}

		this.readableBlueDbEncryptionConfig = encryptionConfig;
		return this;
	}

	/**
	 * Builds the {@link BlueDb} object
	 *
	 * @return the {@link BlueDb} built
	 */
	public BlueDb build() {
		return new ReadWriteDbOnDisk(path, readWriteBlueDbEncryptionConfig);
	}

	/**
	 * Builds the {@link ReadableBlueDb} object
	 *
	 * @return the {@link ReadableBlueDb} built
	 */
	public ReadableBlueDb buildReadOnly() {
		return new ReadableDbOnDisk(path, readableBlueDbEncryptionConfig);
	}

	/**
	 * Replaced by withPath<br><br>
	 * Sets the path you wish to use for the BlueDB data
	 *
	 * @param path the path directory that will contain the BlueDB data
	 * @return itself with the path set
	 */
	@Deprecated
	public BlueDbOnDiskBuilder setPath(Path path) {
		this.path = path;
		return this;
	}

}
