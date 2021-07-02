package org.bluedb.disk.file;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.bluedb.api.encryption.EncryptionServiceWrapper;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.metadata.BlueFileMetadata;
import org.bluedb.api.metadata.BlueFileMetadataKey;
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
		lock = lockManager.acquireWriteLock(path);
		this.path = path;
		this.serializer = serializer;
		this.encryptionService = encryptionService;
		this.dataOutputStream = dataOutputStream;

		metadata = new BlueFileMetadata();
		if (encryptionService.isEncryptionEnabled()) {
			metadata.put(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY, encryptionService.getCurrentEncryptionVersionKey());
		}
	}

	public static <T> BlueObjectOutput<T> createWithoutLockOrSerializer(Path path) throws BlueDbException {
		return createWithoutLock(path, null, new EncryptionServiceWrapper(null));
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

	public void writeBytes(byte[] bytes) throws BlueDbException {
		writeBytes(bytes, true);
	}

	public void writeBytesWithoutEncrypting(byte[] bytes) throws BlueDbException {
		writeBytes(bytes, false);
	}

	private void writeBytes(byte[] bytes, boolean shouldAllowEncryption) throws BlueDbException {
		if (!hasBeenWrittenTo) {
			writeMetadata();
			hasBeenWrittenTo = true;
		}
		try {
			if (shouldAllowEncryption && metadata.containsKey(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY)) {
				String encryptionVersionKey = (String) metadata.get(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY);
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
				String encryptionVersionKey = (String) metadata.get(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY);
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

	public void writeAll(BlueObjectInput<T> input) throws BlueDbException {
		// TODO better protection against hitting overlapping ranges.
		//      There's some protection against this in rollup recovery and 
		//      from single-threaded writes.
		byte[] nextBytes = input.nextWithoutDeserializing();
		while(nextBytes != null && nextBytes.length > 0) {
			writeBytes(nextBytes);
			nextBytes = input.nextWithoutDeserializing();
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

}