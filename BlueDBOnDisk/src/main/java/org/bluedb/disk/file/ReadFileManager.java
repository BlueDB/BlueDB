package org.bluedb.disk.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.encryption.EncryptionServiceWrapper;
import org.bluedb.disk.lock.BlueReadLock;
import org.bluedb.disk.lock.LockManager;
import org.bluedb.disk.metadata.BlueFileMetadata;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.validation.SerializationException;

public class ReadFileManager {

	public static final String TIMESTAMP_VERSION_FORMAT = "yyyy-MM-dd_HH-mm-ss-SSS";

	protected final BlueSerializer serializer;
	protected final EncryptionServiceWrapper encryptionService;
	protected final LockManager<Path> lockManager;

	public ReadFileManager(BlueSerializer serializer, EncryptionServiceWrapper encryptionService) {
		this.serializer = serializer;
		this.encryptionService = encryptionService;
		this.lockManager = new LockManager<>();
	}

	public Object loadObject(BlueReadLock<Path> readLock) throws BlueDbException {
		byte[] fileData = readBytes(readLock);
		if (fileData == null || fileData.length == 0) {
			return null;
		}
		return serializer.deserializeObjectFromByteArray(fileData);
	}

	public Object loadObject(File file) throws BlueDbException {
		return loadObject(file.toPath());
	}

	public Object loadObject(Path path) throws BlueDbException {
		try (BlueReadLock<Path> lock = lockManager.acquireReadLock(path)) {
			return loadObject(lock);
		}
	}

	public Object loadVersionedObject(Path folderPath, String filename) throws BlueDbException, IOException {
		Path newestVersionPath = getNewestVersionPath(folderPath, filename);
		if (newestVersionPath == null) {
			return null;
		}

		return loadObject(newestVersionPath);
	}

	public static Path getNewestVersionPath(Path folderPath, String filename) throws IOException {
		if (!Files.isDirectory(folderPath)) {
			return null;
		}

		return Files.list(folderPath)
				.filter(path -> path.getFileName().toString().startsWith(filename))
				.sorted(Comparator.comparing(Path::getFileName, Comparator.comparing(Path::toString)).reversed())
				.findFirst()
				.orElse(null);
	}

	public <T> BlueObjectInput<T> getBlueInputStream(BlueReadLock<Path> readLock) throws BlueDbException {
		return new BlueObjectInput<T>(readLock, serializer, encryptionService);
	}

	public <T> BlueObjectInput<T> getBlueInputStream(BlueReadLock<Path> readLock, BlueInputStream blueInputStream) throws BlueDbException {
		return new BlueObjectInput<T>(readLock, serializer, encryptionService, blueInputStream);
	}

	public BlueReadLock<Path> getReadLockIfFileExists(Path path) throws BlueDbException {
		BlueReadLock<Path> lock = lockManager.acquireReadLock(path);
		try {
			if (exists(path)) {
				return lock;
			}
		} catch (Throwable t) { // make damn sure we don't hold onto the lock
			lock.release();
			throw new BlueDbException("Error attempting to acquire read lock", t);
		}
		lock.release();
		return null;
	}

	public boolean exists(Path path) {
		return path.toFile().exists();
	}

	public LockManager<Path> getLockManager() {
		return lockManager;
	}


	protected byte[] readBytes(BlueReadLock<Path> readLock) throws BlueDbException {
		Path path = readLock.getKey();
		if (!path.toFile().exists()) {
			return null;
		}
		return readBytes(path);
	}

	protected byte[] readBytes(Path path) throws BlueDbException {
		try (BlueInputStream bis = new BlueDataInputStream(path.toFile())) {
			BlueFileMetadata metadata = readMetadata(bis);
			return encryptionService.decryptOrReturn(metadata, bis.readAllRemainingBytes());
		}
	}
	
	public BlueFileMetadata readMetadata(BlueInputStream bis) throws BlueDbException {
		try {
			bis.mark(Integer.MAX_VALUE);
			Integer objectLength = bis.readNextFourBytesAsInt();
			if (isInvalidObjectLength(objectLength, bis)) {
				bis.resetToLastMark();
				return null; // The file is empty or the unexpected bytes are the first sign of a legacy file
			}
			byte[] metadataBytes = new byte[objectLength];
			bis.readFully(metadataBytes, 0, objectLength);
			Object object;
			try {
				object = serializer.deserializeObjectFromByteArray(metadataBytes);
			} catch (SerializationException e) {
				bis.resetToLastMark();
				return null; // Legacy file without metadata header
			}
			if (!(object instanceof  BlueFileMetadata)) {
				bis.resetToLastMark();
				return null; // Legacy file without metadata header, somehow serialized into an object successfully
			}
			return (BlueFileMetadata) object;
		}
		catch (BlueDbException ex) {
			bis.resetToLastMark();
			return null; // End of file hit when attempting to read in metadata header
		}
		finally {
			bis.mark(0); // Essentially removes the original mark, don't want to tell the input stream to store more in it's buffer than is needed.
		}
	}

	private boolean isInvalidObjectLength(Integer objectLength, BlueInputStream bis) {
		if(objectLength == null || objectLength <= 0) {
			return true;
		}
		
		if(bis.getTotalBytesInStream() > 0 && objectLength > bis.getTotalBytesInStream()) {
            //We know the total length of the input stream and we think the object is larger than that so we know this file is corrupt
            return true;
        }
		
		return false;
	}

}
