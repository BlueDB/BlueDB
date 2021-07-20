package org.bluedb.disk.file;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.text.SimpleDateFormat;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.metadata.BlueFileMetadata;
import org.bluedb.disk.metadata.BlueFileMetadataKey;
import org.bluedb.disk.encryption.EncryptionServiceWrapper;
import org.bluedb.disk.lock.BlueWriteLock;
import org.bluedb.disk.serialization.BlueSerializer;

public class ReadWriteFileManager extends ReadFileManager {

	public ReadWriteFileManager(BlueSerializer serializer, EncryptionServiceWrapper encryptionService) {
		super(serializer, encryptionService);
	}

	public void saveObject(Path path, Object o) throws BlueDbException {
		byte[] bytes = serializer.serializeObjectToByteArray(o);
		FileUtils.ensureDirectoryExists(path.toFile());
		Path tmpPath = FileUtils.createTempFilePath(path);
		try (BlueWriteLock<Path> tempFileLock = lockManager.acquireWriteLock(tmpPath)) {
			writeBytes(tempFileLock, bytes, false);
			try (BlueWriteLock<Path> targetFileLock = lockManager.acquireWriteLock(path)) {
				FileUtils.moveFile(tmpPath, targetFileLock);
			}
		}
	}

	public void saveVersionedObject(Path folderPath, String filename, Object o) throws BlueDbException {
		SimpleDateFormat postfixFormat = new SimpleDateFormat(TIMESTAMP_VERSION_FORMAT);
		String postfix = postfixFormat.format(System.currentTimeMillis());

		saveObject(folderPath.resolve(filename + "_" + postfix), o);
	}

	public void lockMoveFileUnlock(Path src, Path dst) throws BlueDbException {
		try (BlueWriteLock<Path> lock = lockManager.acquireWriteLock(dst)) {
			FileUtils.moveFile(src, lock);
		}
	}

	public void lockDeleteUnlock(File file) {
		Path path = file.toPath();
		try (BlueWriteLock<Path> writeLock = lockManager.acquireWriteLock(path)) {
			FileUtils.deleteFile(writeLock);
		}
	}

	public <T> BlueObjectOutput<T> getBlueOutputStream(BlueWriteLock<Path> writeLock) throws BlueDbException {
		return new BlueObjectOutput<>(writeLock, serializer, encryptionService);
	}

	protected void writeBytes(BlueWriteLock<Path> writeLock, byte[] bytes, boolean forceSkipEncryption) throws BlueDbException {
		Path path = writeLock.getKey();
		try (DataOutputStream dos = FileUtils.openDataOutputStream(path.toFile())) {

			BlueFileMetadata metadata = new BlueFileMetadata();
			if (!forceSkipEncryption && encryptionService.isEncryptionEnabled()) {
				String encryptionVersionKey = encryptionService.getCurrentEncryptionVersionKey();
				metadata.put(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY, encryptionVersionKey);
				bytes = encryptionService.encryptOrThrow(encryptionVersionKey, bytes);
			}
			FileUtils.validateBytes(bytes);
			writeMetadata(metadata, dos, path);
			dos.write(bytes);
		} catch (Throwable t) {
			t.printStackTrace();
			throw new BlueDbException("error writing to file " + path, t);
		}
	}

	protected void writeMetadata(BlueFileMetadata metadata, DataOutputStream dos, Path path) throws BlueDbException {
		try {
			byte[] bytes = serializer.serializeObjectToByteArray(metadata);
			dos.writeInt(bytes.length);
			dos.write(bytes);
		} catch (Throwable t) {
			t.printStackTrace();
			throw new BlueDbException("error writing metadata to file " + path, t);
		}
	}

	public void makeUnencryptedCopy(Path srcPath, Path destPath) throws BlueDbException, IOException {
		// Create file at destPath on the system if needed
		destPath.toFile().getParentFile().mkdirs();
		destPath.toFile().createNewFile();
		
		try (
				DataInputStream dis = FileUtils.openDataInputStream(srcPath.toFile());
				DataOutputStream dos = FileUtils.openDataOutputStream(destPath.toFile())
		) {
			// Write metadata with ENCRYPTION_VERSION_KEY removed
			BlueFileMetadata srcMetadata = readMetadata(dis);
			BlueFileMetadata destMetadata = srcMetadata != null ? srcMetadata : new BlueFileMetadata();
			destMetadata.remove(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY);
			writeMetadata(destMetadata, dos, destPath);

			// Write unencrypted bytes
			byte[] unencryptedBytes = encryptionService.decryptOrReturn(srcMetadata, FileUtils.readAllBytes(dis));
			dos.write(unencryptedBytes);
		} catch (Throwable t) {
			t.printStackTrace();
			throw new BlueDbException("error making unencrypted copy from " + srcPath + " to " + destPath, t);
		}
	}

	public void saveObjectUnencrypted(Path path, Object o) throws BlueDbException {
		byte[] bytes = serializer.serializeObjectToByteArray(o);
		FileUtils.ensureDirectoryExists(path.toFile());
		Path tmpPath = FileUtils.createTempFilePath(path);
		try (BlueWriteLock<Path> tempFileLock = lockManager.acquireWriteLock(tmpPath)) {
			writeBytes(tempFileLock, bytes, true);
			try (BlueWriteLock<Path> targetFileLock = lockManager.acquireWriteLock(path)) {
				FileUtils.moveFile(tmpPath, targetFileLock);
			}
		}
	}

}
