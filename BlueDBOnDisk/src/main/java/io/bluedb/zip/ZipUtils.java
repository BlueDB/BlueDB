package io.bluedb.zip;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class ZipUtils {

	public static void zipFile(Path fileToZip, Path destination) throws IOException {
		zipFile(fileToZip.toString(), destination.toString());
	}

	public static void zipFile(String fileToZip, String destinationFilename) throws IOException {
		try (ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(destinationFilename))) {
			File file = new File(fileToZip);
			zipFile(file, file.getName(), zipOut);
		}
	}

	public static void extractFiles(Path zipDirectory, Path outputDirectory) throws IOException {
		extractFiles(zipDirectory.toString(), outputDirectory.toString());
	}

	public static void extractFiles(String zipDirectory, String outputDirectory) throws IOException {
		try (ZipInputStream zis = new ZipInputStream(new BufferedInputStream(new FileInputStream(zipDirectory)))) {
			ZipEntry entry = null;
			while ((entry = zis.getNextEntry()) != null) {
				extractEntryContents(zis, entry, outputDirectory);
			}
			zis.closeEntry();
		}
	}

	private static void zipFile(File fileToZip, String fileName, ZipOutputStream zipOut) throws IOException {
		if (fileToZip.isDirectory()) {
			File[] children = fileToZip.listFiles();
			for (File childFile : children) {
				zipFile(childFile, fileName + File.separator + childFile.getName(), zipOut);
			}
			return;
		}
		try (FileInputStream fis = new FileInputStream(fileToZip)) {
			ZipEntry zipEntry = new ZipEntry(fileName);
			zipOut.putNextEntry(zipEntry);
			copyBytes(fis, zipOut);
		}
	}

	private static void extractEntryContents(ZipInputStream zis, ZipEntry entry, String outputDirectory) throws IOException {
		Path path = Paths.get(outputDirectory, entry.getName());
		Files.createDirectories(path.getParent());
		copyBytesIfNotEmpty(zis, path);
	}

	private static void copyBytes(InputStream inputStream, OutputStream outputStream) throws IOException {
		byte[] buffer = new byte[1024];
		int bytesRead;
		while ((bytesRead = inputStream.read(buffer)) != -1) {
			outputStream.write(buffer, 0, bytesRead);
		}
	}

	private static void copyBytesIfNotEmpty(InputStream inputStream, Path destination) throws IOException {
		byte[] buffer = new byte[1024];
		int bytesRead = inputStream.read(buffer);
		
		if(bytesRead != -1) {
			try (BufferedOutputStream bos = new BufferedOutputStream(Files.newOutputStream(destination))) {
				bos.write(buffer, 0, bytesRead);
				
				while ((bytesRead = inputStream.read(buffer)) != -1) {
					bos.write(buffer, 0, bytesRead);
				}
			}
		}
	}
}