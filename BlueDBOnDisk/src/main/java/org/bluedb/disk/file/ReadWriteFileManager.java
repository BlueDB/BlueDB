package org.bluedb.disk.file;

import java.io.DataOutputStream;
import java.io.File;
import java.nio.file.Path;
import java.text.SimpleDateFormat;

import org.bluedb.api.encryption.EncryptionServiceWrapper;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.metadata.BlueFileMetadataKey;
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
			writeBytes(tempFileLock, bytes);
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

	protected void writeBytes(BlueWriteLock<Path> writeLock, byte[] bytes) throws BlueDbException {
		Path path = writeLock.getKey();
		File file = path.toFile();
		try (DataOutputStream dos = FileUtils.openDataOutputStream(file)) {
			if (metadata.containsKey(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY)) {
				String encryptionVersionKey = (String) metadata.get(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY);
				bytes = encryptionService.encryptOrThrow(encryptionVersionKey, bytes);
			}
			FileUtils.validateBytes(bytes);
			writeMetadata(dos, path);
			dos.write(bytes);
		} catch (Throwable t) {
			t.printStackTrace();
			throw new BlueDbException("error writing to file " + path, t);
		}
	}

	protected void writeMetadata(DataOutputStream dos, Path path) throws BlueDbException {
		try {
			byte[] bytes = serializer.serializeObjectToByteArray(metadata);
			int len = bytes.length;
			dos.writeInt(len);
			dos.write(bytes);
		} catch (Throwable t) {
			t.printStackTrace();
			throw new BlueDbException("error writing metadata to file " + path, t);
		}
	}
}
