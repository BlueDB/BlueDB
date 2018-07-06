package io.bluedb.disk.file;

import java.io.EOFException;
import java.io.File;
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
import io.bluedb.disk.serialization.BlueSerializer;

public class FileManager {
	private final BlueSerializer serializer;
	private final LockManager<Path> lockManager;

	public FileManager(BlueSerializer serializer) {
		this.serializer = serializer;
		lockManager = new LockManager<Path>();
	}

	public List<File> listFiles(Path path, String suffix) {
		File folder = path.toFile();
		if (!folder.exists()) {
			return new ArrayList<>();
		}
		File[] filesInFolder = folder.listFiles();
		return filterFilesWithSuffix(filesInFolder, suffix);
	}

	public Object loadObject(BlueReadLock<Path> readLock) throws BlueDbException {
		byte[] fileData = readBytes(readLock);
		if (fileData == null || fileData.length == 0) {
			return null;
		}
		return serializer.deserializeObjectFromByteArray(fileData);
	}

	public Object loadObject(Path path) throws BlueDbException {
		try (BlueReadLock<Path> lock = lockManager.acquireReadLock(path)){
			return loadObject(lock);
		}
	}

	public void saveObject(Path path, Object o) throws BlueDbException {
		byte[] bytes = serializer.serializeObjectToByteArray(o);
		Path tmpPath = createTempFilePath(path);
		try (BlueWriteLock<Path> tempFileLock = lockManager.acquireWriteLock(tmpPath)) {
			writeBytes(tempFileLock, bytes);
			try (BlueWriteLock<Path> targetFileLock = lockManager.acquireWriteLock(path)) {
				moveFile(tmpPath, targetFileLock);
			}
		}
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

	public <T> ArrayList<T> loadList(BlueReadLock<Path> readLock) throws BlueDbException {
		ArrayList<T> items = new ArrayList<>();
		try(BlueObjectInput<T> inputStream = getBlueInputStream(readLock)) {
			while(inputStream.hasNext()) {
				items.add(inputStream.next());
			}
		} catch(EOFException e) {
		} catch (IOException e1) {
			e1.printStackTrace();
			throw new BlueDbException("IOException while loading list", e1); // TODO handle this better
		}
		return items;
	}

	public <T> void saveList(BlueWriteLock<Path> writeLock, List<T> items) throws BlueDbException {
		try(BlueObjectOutput<T> outputStream = getBlueOutputStream(writeLock)) {
			for (T item: items) {
				outputStream.write(item);
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new BlueDbException("IOException while saving list", e); // TODO handle this better
		}
	}

	public <T> BlueObjectOutput<T> getBlueOutputStream(BlueWriteLock<Path> writeLock) throws BlueDbException {
		return new BlueObjectOutput<T>(writeLock, serializer);
	}

	public <T> BlueObjectInput<T> getBlueInputStream(BlueReadLock<Path> readLock) throws BlueDbException {
		return new BlueObjectInput<T>(readLock, serializer);
	}

	protected static List<File> filterFilesWithSuffix(File[] files, String suffix) {
		List<File> results = new ArrayList<>();
		for (File file: files) {
			if(file.getName().endsWith(suffix)) {
				results.add(file);
			}
		}
		return results;
	}

	protected static void ensureDirectoryExists(File file) {
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

	public void lockMoveFileUnlock(Path src, Path dst) throws BlueDbException {
		try (BlueWriteLock<Path> lock = lockManager.acquireWriteLock(dst)) {
			moveFile(src, lock);
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

	public static boolean deleteFile(BlueWriteLock<Path> writeLock) {
		Path path = writeLock.getKey();
		return path.toFile().delete();
	}

	private byte[] readBytes(BlueReadLock<Path> readLock) throws BlueDbException {
		Path path = readLock.getKey();
		try {
			if (!path.toFile().exists()) {
				return null;
			}
			return Files.readAllBytes(path);
		} catch (IOException e) {
			e.printStackTrace();
			throw new BlueDbException("error reading bytes from " + path);
		}
	}

	private void writeBytes(BlueWriteLock<Path> writeLock, byte[] bytes) throws BlueDbException {
		Path path = writeLock.getKey();
		File file = path.toFile();
		ensureDirectoryExists(file);
		try (FileOutputStream fos = new FileOutputStream(file)) {
			fos.write(bytes);
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new BlueDbException("error writing to file " + path, e);
		}
	}
}
