package io.bluedb.disk.file;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
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

	public Object loadObject(Path path) throws BlueDbException {
		byte[] fileData = getLatchAndReadBytes(path);
		if (fileData == null || fileData.length == 0) {
			return null;
		}
		return serializer.deserializeObjectFromByteArray(fileData);
	}

	public void saveObject(Path path, Object o) throws BlueDbException {
		byte[] bytes = serializer.serializeObjectToByteArray(o);
		lockManager.acquire(path);
		try {
			writeBytes(path, bytes);
		} finally {
			lockManager.release(path);
		}
	}

	private byte[] getLatchAndReadBytes(Path path) throws BlueDbException {
		lockManager.acquire(path);
		try {
			return readBytes(path);
		} finally {
			lockManager.release(path);
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
		File tmpFile = Paths.get(path.toString() + "_tmp").toFile();
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
