package org.bluedb.disk.file;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.encryption.EncryptionUtils;
import org.bluedb.disk.metadata.BlueFileMetadata;
import org.bluedb.disk.metadata.BlueFileMetadataKey;
import org.bluedb.disk.encryption.EncryptionServiceWrapper;
import org.bluedb.disk.lock.BlueWriteLock;
import org.bluedb.disk.lock.LockManager;
import org.bluedb.disk.serialization.BlueSerializer;

public class BlueObjectOutput<T> implements Closeable {

	private final BlueWriteLock<Path> lock;
	private final Path path;
	private final BlueSerializer serializer;
	private final EncryptionServiceWrapper encryptionService;
	private final DataOutputStream dataOutputStream;
	private final BlueFileMetadata metadata;

	private boolean hasBeenWrittenTo = false;

	public BlueObjectOutput(BlueWriteLock<Path> writeLock, BlueSerializer serializer, EncryptionServiceWrapper encryptionService) throws BlueDbException {
		try {
			lock = writeLock;
			path = lock.getKey();
			this.serializer = serializer;
			this.encryptionService = encryptionService;
			File file = path.toFile();
			FileUtils.ensureDirectoryExists(file);
			dataOutputStream = FileUtils.openDataOutputStream(file);

			metadata = new BlueFileMetadata();
			if (encryptionService.isEncryptionEnabled()) {
				metadata.put(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY, encryptionService.getCurrentEncryptionVersionKey());
			}
		} catch (Throwable t) {
			close();
			throw new BlueDbException(t.getMessage(), t);
		}
	}

	protected static <T> BlueObjectOutput<T> getTestOutput(Path path, BlueSerializer serializer, EncryptionServiceWrapper encryptionService, DataOutputStream dataOutputStream) {
		return new BlueObjectOutput<>(path, serializer, encryptionService, dataOutputStream);
	}

	private BlueObjectOutput(Path path, BlueSerializer serializer, EncryptionServiceWrapper encryptionService, DataOutputStream dataOutputStream) {
		LockManager<Path> lockManager = new LockManager<>();
		this.lock = lockManager.acquireWriteLock(path);
		this.path = path;
		this.serializer = serializer;
		this.dataOutputStream = dataOutputStream;
		this.metadata = new BlueFileMetadata();

		if (encryptionService == null) {
			encryptionService = new EncryptionServiceWrapper(null);
		}
		this.encryptionService = encryptionService;
		if (this.encryptionService.isEncryptionEnabled()) {
			metadata.put(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY, encryptionService.getCurrentEncryptionVersionKey());
		}
	}

	public static <T> BlueObjectOutput<T> createWithoutLock(Path path, BlueSerializer serializer, EncryptionServiceWrapper encryptionService) throws BlueDbException {
		return new BlueObjectOutput<>(path, serializer, encryptionService);
	}

	private BlueObjectOutput(Path path, BlueSerializer serializer, EncryptionServiceWrapper encryptionService) throws BlueDbException {
		try {
			this.lock = null;
			this.path = path;
			this.serializer = serializer;
			this.encryptionService = encryptionService;
			this.dataOutputStream = FileUtils.openDataOutputStream(path.toFile());

			metadata = new BlueFileMetadata();
			if (encryptionService.isEncryptionEnabled()) {
				metadata.put(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY, encryptionService.getCurrentEncryptionVersionKey());
			}
		} catch (IOException e) {
			throw new BlueDbException("Failed to create BlueObjectOutput for path " + path, e);
		}
	}

	public void writeBytesAndAllowEncryption(byte[] unencryptedBytes) throws BlueDbException {
		writeBytes(unencryptedBytes, false);
	}

	public void writeBytesAndForceSkipEncryption(byte[] bytes) throws BlueDbException {
		writeBytes(bytes, true);
	}

	private void writeBytes(byte[] bytes, boolean forceSkipEncryption) throws BlueDbException {
		if (!hasBeenWrittenTo) {
			writeMetadata();
			hasBeenWrittenTo = true;
		}
		try {
			if (!forceSkipEncryption && metadata.containsKey(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY)) {
				String encryptionVersionKey = metadata.get(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY).get();
				bytes = encryptionService.encryptOrThrow(encryptionVersionKey, bytes);
			}
			FileUtils.validateBytes(bytes);
			int len = bytes.length;
			dataOutputStream.writeInt(len);
			dataOutputStream.write(bytes);
		} catch (Throwable t) {
			t.printStackTrace();
			throw new BlueDbException("error writing to file " + path, t);
		}
	}

	public void write(T value) throws BlueDbException {
		if (!hasBeenWrittenTo) {
			writeMetadata();
			hasBeenWrittenTo = true;
		}
		if (value == null) {
			throw new BlueDbException("cannot write null to " + this.getClass().getSimpleName());
		}
		try {
			byte[] bytes = serializer.serializeObjectToByteArray(value);
			if (metadata.containsKey(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY)) {
				String encryptionVersionKey = metadata.get(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY).get();
				bytes = encryptionService.encryptOrThrow(encryptionVersionKey, bytes);
			}
			int len = bytes.length;
			dataOutputStream.writeInt(len);
			dataOutputStream.write(bytes);
		} catch (Throwable t) {
			t.printStackTrace();
			throw new BlueDbException("error writing to file " + path, t);
		}
	}

	public void writeAll(BlueObjectInput<?> input) throws BlueDbException {
		// TODO better protection against hitting overlapping ranges.
		//      There's some protection against this in rollup recovery and 
		//      from single-threaded writes.
		boolean shouldSkipEncryptionForUnchangedData = EncryptionUtils.shouldWriterSkipEncryptionForUnchangedDataUsingRawBytes(input.getMetadata(), getMetadata());
		byte[] nextRawBytes = input.nextRawBytesWithoutDeserializing();
		while (nextRawBytes != null && nextRawBytes.length > 0) {
			if (shouldSkipEncryptionForUnchangedData) {
				writeBytesAndForceSkipEncryption(input.getLastRawBytes());
			} else {
				writeBytesAndAllowEncryption(input.getLastUnencryptedBytes());
			}
			nextRawBytes = input.nextRawBytesWithoutDeserializing();
		}
	}

	@Override
	public void close() {
		if (dataOutputStream != null) {
			try {
				dataOutputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		if (lock != null) {
			lock.close();
		}
	}

	private void writeMetadata() throws BlueDbException {
		try {
			byte[] bytes = serializer.serializeObjectToByteArray(metadata);
			int len = bytes.length;
			dataOutputStream.writeInt(len);
			dataOutputStream.write(bytes);
		} catch (Throwable t) {
			t.printStackTrace();
			throw new BlueDbException("error writing metadata to file " + path, t);
		}
	}

	public BlueFileMetadata getMetadata() {
		return metadata;
	}

	public void setMetadataValue(BlueFileMetadataKey key, String value) {
		metadata.put(key, value);
	}

}