package io.bluedb.disk.file;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.lock.BlueWriteLock;

public class FileUtils {
	
	private final static String TEMP_FILE_PREFIX = "_tmp_";

	protected FileUtils() {}  // just to get test coverage to 100%

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
			return Paths.get(parentFile.toPath().toString(), TEMP_FILE_PREFIX + originalPath.getFileName().toString());
		} else {
			return Paths.get(TEMP_FILE_PREFIX + originalPath.getFileName().toString());
		}
	}

	public static boolean isTempFile(File file) {
		if (file == null) {
			return false;
		}
		String fileName = file.getName();
		return fileName.startsWith(TEMP_FILE_PREFIX);
	}

	public static void moveFile(Path src, BlueWriteLock<Path> lock) throws BlueDbException {
		Path dst = lock.getKey();
		try {
			Blutils.tryMultipleTimes(5, () -> Files.move(src, dst, StandardCopyOption.ATOMIC_MOVE));
		} catch (Throwable e) {
			e.printStackTrace();
			throw new BlueDbException("trouble moving file from "  + src.toString() + " to " + dst.toString() , e);
		}
	}

	public static void moveWithoutLock(Path src, Path dst) throws BlueDbException {
		try {
			ensureDirectoryExists(dst.toFile());
			Blutils.tryMultipleTimes(5, () -> Files.move(src, dst, StandardCopyOption.ATOMIC_MOVE));
		} catch (Throwable e) {
			e.printStackTrace();
			throw new BlueDbException("trouble moving file from "  + src.toString() + " to " + dst.toString() , e);
		}
	}

	public static void copyFileWithoutLock(Path src, Path dst) throws BlueDbException {
		try {
			ensureDirectoryExists(dst.toFile());
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
}
