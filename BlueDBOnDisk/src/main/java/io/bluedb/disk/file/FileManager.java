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

	public <T> ArrayList<T> loadList(Path path) throws BlueDbException {
		ArrayList<T> items = new ArrayList<>();
		try(BlueObjectInputStream<T> inputStream = getBlueInputStream(path)) {
			while(true) {
				items.add(inputStream.next());
			}
		} catch(EOFException e) {
		} catch (IOException e1) {
			e1.printStackTrace();
			throw new BlueDbException("IOException while loading list", e1); // TODO handle this better
		}
		return items;
	}

	public <T> void saveList(Path path, List<T> items) throws BlueDbException {
		try(BlueObjectOutputStream<T> outputStream = getBlueOutputStream(path)) {
			for (T item: items) {
				outputStream.write(item);
			}
			outputStream.commit();
		} catch (IOException e) {
			e.printStackTrace();
			throw new BlueDbException("IOException while saving list", e); // TODO handle this better
		}
	}

	public Object loadObject(Path path) throws BlueDbException {
		byte[] fileData = getLatchAndReadBytes(path);
		if (fileData == null || fileData.length == 0) {
			return null;
		}
		return serializer.deserializeObjectFromByteArray(fileData);
	}

	public void saveObject(Path path, Object o) throws BlueDbException {
		byte[] bytes = serializer.serializeObjectToByteArray(o);
		lockManager.acquireWriteLock(path);
		try {
			writeBytes(path, bytes);
		} finally {
			lockManager.releaseWriteLock(path);
		}
	}

	public static List<File> getFolderContents(File folder) {
		File[] folderContentsArray = folder.listFiles();
		if (folderContentsArray == null) {
			return new ArrayList<>();
		}
		return Arrays.asList(folderContentsArray);
	}

	public void lockMoveFileUnlock(Path src, Path dst) throws BlueDbException {
		lockManager.acquireWriteLock(dst);
		try {
			moveFile(src, dst);
		} finally {
			lockManager.releaseWriteLock(dst);
		}
	}

	public <T> BlueObjectInputStream<T> getBlueInputStream(Path path) throws BlueDbException {
		return new BlueObjectInputStream<T>(path, serializer, lockManager);
	}

	public <T> BlueObjectOutputStream<T> getBlueOutputStream(Path path) throws BlueDbException {
		return new BlueObjectOutputStream<T>(path, serializer, this);
	}

	private byte[] getLatchAndReadBytes(Path path) throws BlueDbException {
		lockManager.acquireReadLock(path);
		try {
			return readBytes(path);
		} finally {
			lockManager.releaseReadLock(path);
		}
	}

	protected static void moveFile(Path src, Path dst) throws BlueDbException {
		try {
			Files.move(src, dst, StandardCopyOption.ATOMIC_MOVE);
		} catch (IOException e) {
			e.printStackTrace();
			throw new BlueDbException("trouble moving file from "  + src.toString() + " to " + dst.toString() , e);
		}		
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

	protected static Path createTempFilePath(Path originalPath) {
		File parentFile = originalPath.toFile().getParentFile();
		if (parentFile != null) {
			return Paths.get(parentFile.toPath().toString(), "_tmp_" + originalPath.getFileName().toString());
		} else {
			return Paths.get("_tmp_" + originalPath.getFileName().toString());
		}
	}

	protected static void ensureDirectoryExists(File file) {
		File parent = file.getParentFile();
		if (parent != null) {
			parent.mkdirs();
		}
	}

	private byte[] readBytes(Path path) throws BlueDbException {
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

	private void writeBytes(Path path, byte[] bytes) throws BlueDbException {
		File tmpFile = createTempFilePath(path).toFile();
		File targetFile = path.toFile();
		ensureDirectoryExists(targetFile);
		try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
			fos.write(bytes);
			fos.close();
			Files.move(tmpFile.toPath(), path, StandardCopyOption.ATOMIC_MOVE);
		} catch (IOException e) {
			e.printStackTrace();
			throw new BlueDbException("error writing to file " + path, e);
		}
	}
}
