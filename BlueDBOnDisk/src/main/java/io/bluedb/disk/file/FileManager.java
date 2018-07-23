package io.bluedb.disk.file;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.lock.BlueReadLock;
import io.bluedb.disk.lock.BlueWriteLock;
import io.bluedb.disk.lock.LockManager;
import io.bluedb.disk.serialization.BlueSerializer;

public class FileManager {
	private final BlueSerializer serializer;
	private final LockManager<Path> lockManager;

	public FileManager(BlueSerializer serializer) {
		this.serializer = serializer;
		lockManager = new LockManager<Path>();
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
		try (BlueReadLock<Path> lock = lockManager.acquireReadLock(path)){
			return loadObject(lock);
		}
	}

	public void saveObject(Path path, Object o) throws BlueDbException {
		byte[] bytes = serializer.serializeObjectToByteArray(o);
		ensureDirectoryExists(path.toFile());
		Path tmpPath = createTempFilePath(path);
		try (BlueWriteLock<Path> tempFileLock = lockManager.acquireWriteLock(tmpPath)) {
			writeBytes(tempFileLock, bytes);
			try (BlueWriteLock<Path> targetFileLock = lockManager.acquireWriteLock(path)) {
				moveFile(tmpPath, targetFileLock);
			}
		}
	}

	public void lockMoveFileUnlock(Path src, Path dst) throws BlueDbException {
		try (BlueWriteLock<Path> lock = lockManager.acquireWriteLock(dst)) {
			moveFile(src, lock);
		}
	}

	public <T> BlueObjectOutput<T> getBlueOutputStream(BlueWriteLock<Path> writeLock) throws BlueDbException {
		return new BlueObjectOutput<T>(writeLock, serializer);
	}

	public <T> BlueObjectInput<T> getBlueInputStream(BlueReadLock<Path> readLock) throws BlueDbException {
		return new BlueObjectInput<T>(readLock, serializer);
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

	public static List<File> getFolderContents(File folder) {
		File[] folderContentsArray = folder.listFiles();
		if (folderContentsArray == null) {
			return new ArrayList<>();
		}
		return Arrays.asList(folderContentsArray);
	}

	public static List<File> getFolderContents(File folder, FileFilter filter) {
		File[] folderContentsArray = folder.listFiles(filter);
		if (folderContentsArray == null) {
			return new ArrayList<>();
		}
		return Arrays.asList(folderContentsArray);
	}

	public static List<File> getFolderContents(Path path, String suffix) {
		FileFilter endsWithSuffix = (f) -> f.toPath().toString().endsWith(suffix);
		return getFolderContents(path.toFile(), endsWithSuffix);
	}

	public static void ensureFileExists(Path path) throws BlueDbException {
		File file = path.toFile();
		ensureDirectoryExists(file);
		if (!file.exists()) {
			createEmptyFile(path);
		}
	}

	public static void createEmptyFile(Path path) throws BlueDbException {
		try {
			path.toFile().createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
			throw new BlueDbException("can't create file " + path);
		}
	}

	public static void ensureDirectoryExists(File file) {
		File parent = file.getParentFile();
		if (parent != null) {
			parent.mkdirs();
		}
	}

	public static Path createTempFilePath(Path originalPath) {
		File parentFile = originalPath.toFile().getParentFile();
		if (parentFile != null) {
			return Paths.get(parentFile.toPath().toString(), "_tmp_" + originalPath.getFileName().toString());
		} else {
			return Paths.get("_tmp_" + originalPath.getFileName().toString());
		}
	}

	public static void moveFile(Path src, BlueWriteLock<Path> lock) throws BlueDbException {
		Path dst = lock.getKey();
		try {
			Files.move(src, dst, StandardCopyOption.ATOMIC_MOVE);
		} catch (IOException e) {
			e.printStackTrace();
			throw new BlueDbException("trouble moving file from "  + src.toString() + " to " + dst.toString() , e);
		}
	}

	public static void moveWithoutLock(Path src, Path dst) throws BlueDbException {
		try {
			// TODO test to ensure parent exists
			dst.toFile().getParentFile().mkdirs();
			Files.move(src, dst, StandardCopyOption.ATOMIC_MOVE);
		} catch (IOException e) {
			e.printStackTrace();
			throw new BlueDbException("trouble moving file from "  + src.toString() + " to " + dst.toString() , e);
		}
	}

	public static void copyFileWithoutLock(Path src, Path dst) throws BlueDbException {
		try {
			// TODO use atomic move and don't create a directory that has to be replaced anyway
			dst.toFile().getParentFile().mkdirs();
			Files.copy(src, dst, StandardCopyOption.COPY_ATTRIBUTES);
		} catch (IOException e) {
			e.printStackTrace();
			throw new BlueDbException("Can't copy '" + src + "' to '" + dst + "'.", e);
		}
	}

	public static void copyDirectoryWithoutLock(Path src, Path dst) throws BlueDbException {
		if (!src.toFile().isDirectory()) {
			throw new BlueDbException(src + " is not a directory.");
		}
		dst.toFile().mkdirs();
		for (File file: src.toFile().listFiles()) {
			if (file.isDirectory()) {
				Path path = file.toPath();
				Path target = dst.resolve(src.relativize(path));
				copyDirectoryWithoutLock(path, target);
			} else {
				Path path = file.toPath();
				Path target = dst.resolve(src.relativize(path));
				copyFileWithoutLock(path, target);
			}
		}
	}

	public static boolean deleteFile(BlueWriteLock<Path> writeLock) {
		Path path = writeLock.getKey();
		return path.toFile().delete();
	}

	protected byte[] readBytes(BlueReadLock<Path> readLock) throws BlueDbException {
		Path path = readLock.getKey();
		if (!path.toFile().exists()) {
			return null;
		}
		return readBytes(path);
	}

	protected byte[] readBytes(Path path) throws BlueDbException {
		try {
			return Files.readAllBytes(path);
		} catch (IOException e) {
			e.printStackTrace();
			throw new BlueDbException("error reading bytes from " + path);
		}
	}

	protected void writeBytes(BlueWriteLock<Path> writeLock, byte[] bytes) throws BlueDbException {
		Path path = writeLock.getKey();
		File file = path.toFile();
		try (FileOutputStream fos = new FileOutputStream(file)) {
			fos.write(bytes);
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new BlueDbException("error writing to file " + path, e);
		}
	}
}
