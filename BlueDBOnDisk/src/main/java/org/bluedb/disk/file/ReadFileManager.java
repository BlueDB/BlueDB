package org.bluedb.disk.file;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.metadata.BlueFileMetadata;
import org.bluedb.disk.encryption.EncryptionServiceWrapper;
import org.bluedb.disk.lock.BlueReadLock;
import org.bluedb.disk.lock.LockManager;
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
		try (DataInputStream dis = FileUtils.openDataInputStream(path.toFile())) {
			BlueFileMetadata metadata = readMetadata(dis);
			return encryptionService.decryptOrReturn(metadata, FileUtils.readAllBytes(dis));
		} catch (IOException e) {
			e.printStackTrace();
			throw new BlueDbException("error reading bytes from " + path);
		}
	}

	protected BlueFileMetadata readMetadata(DataInputStream dis) throws IOException {
		try {
			dis.mark(Integer.MAX_VALUE);
			Integer objectLength = FileUtils.readInt(dis);
			if (objectLength == null || objectLength <= 0) {
				dis.reset();
				return null; // The file is empty or the unexpected bytes are the first sign of a legacy file
			}
			byte[] metadataBytes = new byte[objectLength];
			dis.readFully(metadataBytes, 0, objectLength);
			Object object;
			try {
				object = serializer.deserializeObjectFromByteArray(metadataBytes);
			} catch (SerializationException e) {
				dis.reset();
				return null; // Legacy file without metadata header
			}
			if (!(object instanceof  BlueFileMetadata)) {
				dis.reset();
				return null; // Legacy file without metadata header, somehow serialized into an object successfully
			}
			return (BlueFileMetadata) object;
		}
		catch (EOFException ex) {
			dis.reset();
			return null; // End of file hit when attempting to read in metadata header
		}
		finally {
			dis.mark(0); // Essentially removes the original mark, don't want to tell the input stream to store more in it's buffer than is needed.
		}
	}

}
