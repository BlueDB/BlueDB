package org.bluedb.disk.file;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.text.SimpleDateFormat;

import org.bluedb.api.encryption.EncryptionServiceWrapper;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.lock.BlueWriteLock;
import org.bluedb.disk.serialization.BlueSerializer;

public class ReadWriteFileManager extends ReadFileManager {

	public ReadWriteFileManager(BlueSerializer serializer, EncryptionServiceWrapper encryptionService) {
		super(serializer, encryptionService);
	}

	public void saveObject(Path path, Object o) throws BlueDbException {
		byte[] bytes = serializer.serializeObjectToByteArray(o);
		bytes = encryptionService.encryptOrReturn(bytes);
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
		return new BlueObjectOutput<T>(writeLock, serializer, encryptionService);
	}

	protected void writeBytes(BlueWriteLock<Path> writeLock, byte[] bytes) throws BlueDbException {
		Path path = writeLock.getKey();
		File file = path.toFile();
		try (FileOutputStream fos = new FileOutputStream(file)) {
			FileUtils.validateBytes(bytes);
			fos.write(bytes);
			fos.close();
		} catch (Throwable t) {
			t.printStackTrace();
			throw new BlueDbException("error writing to file " + path, t);
		}
	}
}
