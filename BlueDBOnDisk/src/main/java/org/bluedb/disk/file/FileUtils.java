package org.bluedb.disk.file;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.lock.BlueWriteLock;

public class FileUtils {
	
	private final static String TEMP_FILE_PREFIX = "_tmp_";
	private final static FileFilter IS_NOT_TEMP_FILE = (f) -> !f.getName().startsWith(TEMP_FILE_PREFIX);
	private final static FileFilter IS_TEMP_FILE = (f) -> f.getName().startsWith(TEMP_FILE_PREFIX);

	protected FileUtils() {}  // just to get test coverage to 100%

	public static List<File> getSubFolders(File folder) {
		File[] subfolders = folder.listFiles( (f) -> f.isDirectory() );
		return toList(subfolders);
	}

	public static List<File> getFolderContentsExcludingTempFiles(File folder) {
		File[] folderContentsArray = folder.listFiles(IS_NOT_TEMP_FILE);
		return toList(folderContentsArray);
	}

	public static List<File> getFolderContentsExcludingTempFiles(Path path, String suffix) {
		FileFilter endsWithSuffix = (f) -> f.toPath().toString().endsWith(suffix);
		return getFolderContentsExcludingTempFiles(path.toFile(), endsWithSuffix);
	}

	public static DirectoryStream<Path> getFolderContentsExcludingTempFilesAsStream(Path path, String suffix) throws IOException {
		FileFilter endsWithSuffix = (f) -> f.toPath().toString().endsWith(suffix);
		return getFolderContentsExcludingTempFilesAsStream(path.toFile(), endsWithSuffix);
	}

	public static List<File> getFolderContentsExcludingTempFiles(File folder, FileFilter filter) {
		List<File> files = new LinkedList<File>();
		try(DirectoryStream<Path> stream = getFolderContentsExcludingTempFilesAsStream(folder, filter)){
			stream.iterator().forEachRemaining(p -> files.add(p.toFile()));
		} catch (IOException ex) {
			
		}
		
		return files;
	}
	
	private static DirectoryStream<Path> getFolderContentsExcludingTempFilesAsStream(File folder, FileFilter filter) throws IOException {
		Filter<Path> passesFilterAndIsNotTempFile = (p) -> filter.accept(p.toFile()) && IS_NOT_TEMP_FILE.accept(p.toFile());
		return Files.newDirectoryStream(Paths.get(folder.getAbsolutePath()), passesFilterAndIsNotTempFile );
	}
	
	public static DirectoryStream<Path> getTempFolderContentsAsStream(File folder, FileFilter filter) throws IOException {
		Filter<Path> passesFilterAndIsNotTempFile = (p) -> filter.accept(p.toFile()) && IS_TEMP_FILE.accept(p.toFile());
		return Files.newDirectoryStream(Paths.get(folder.getAbsolutePath()), passesFilterAndIsNotTempFile );
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
	
	public static Path createTempFilePathInDirectory(Path directory, String filename) throws BlueDbException {
		Path path = directory.resolve(TEMP_FILE_PREFIX + filename);
		path.toFile().deleteOnExit();
		return path;
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
	
	public static boolean exists(Path path) {
		if(path == null) {
			return false;
		}
		return path.toFile().exists();
	}
	
	public static boolean isEmpty(Path path) {
		if(path == null || !path.toFile().exists()) {
			return true;
		}
		return path.toFile().length() == 0;
	}

	public static void moveFile(Path src, BlueWriteLock<Path> lock) throws BlueDbException {
		Path dst = lock.getKey();
		try {
			Blutils.tryMultipleTimes(5, () -> {
				validateFileBytes(src);
				Blutils.tryMultipleTimes(5, () -> Files.move(src, dst, StandardCopyOption.ATOMIC_MOVE));
				validateFileBytes(dst);
				return true;
			});
		} catch (Throwable e) {
			e.printStackTrace();
			throw new BlueDbException("trouble moving file from "  + src.toString() + " to " + dst.toString() , e);
		}
	}

	public static void moveWithoutLock(Path src, Path dst) throws BlueDbException {
		try {
			ensureDirectoryExists(dst.toFile());
			Blutils.tryMultipleTimes(5, () -> { 
				validateFileBytes(src);
				Files.move(src, dst, StandardCopyOption.ATOMIC_MOVE);
				validateFileBytes(dst);
				return true;
			});
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

	private static List<File> toList(File[] files) {
		if (files == null) {
			return new ArrayList<>();
		}
		return Arrays.asList(files);
	}

	public static void deleteIfExistsWithoutLock(Path path) throws BlueDbException {
		try {
			Files.deleteIfExists(path);
		} catch (IOException e) {
			throw new BlueDbException("Failed to delete file " + path, e);
		}
	}
	
	public static DataOutputStream openDataOutputStream(File file) throws IOException {
		return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
	}
	
	public static void validateFileBytes(Path file) throws BlueDbException {
		try(FileInputStream fis = new FileInputStream(file.toFile())) {
			byte[] buffer = new byte[1024];
			
			int length = fis.read(buffer);
			if(length < 0) {
				return; //An empty file is valid
			}
			
			while(length > 0) {
				if(!areAllBytesZeros(buffer, length)) {
					return;
				}
				length = fis.read(buffer);
			}
			throw new BlueDbException("Invalid File Bytes: Files must contain non zero values.");
		} catch (Throwable t) {
			throw new BlueDbException("Validation of file bytes failed. File: " + file, t);
		}
	}
	
	public static boolean validateBytes(byte[] bytes) throws BlueDbException {
		if(bytes == null) {
			throw new BlueDbException("Invalid bytes: Cannot save a null byte array");
		} else if(bytes.length <= 0) {
			throw new BlueDbException("Invalid bytes: Cannot save an empty byte array");
		}
		
		if(areAllBytesZeros(bytes)) {
			throw new BlueDbException("Invalid bytes: Cannot save bytes that only contain zeros");
		}
		
		return true;
	}
	
	public static boolean areAllBytesZeros(byte[] bytes) {
		if(bytes == null || bytes.length == 0) {
			return false;
		}
		
		return areAllBytesZeros(bytes, bytes.length);
	}

	public static boolean areAllBytesZeros(byte[] bytes, int length) {
		if(bytes == null || bytes.length == 0) {
			return false;
		}
		
		for(int i = 0; i < bytes.length && i < length; i++) {
			if(bytes[i] != 0) {
				return false;
			}
		}
		return true;
	}
}
