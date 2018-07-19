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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class ZipUtils {

	public static void zipFile(Path fileToZip, Path destination) throws IOException {
		try (ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(destination.toString()))) {
			File file = fileToZip.toFile();
			zipFile(file, file.getName(), zipOut);
		}
	}

	public static void extractFiles(Path zipDirectory, Path outputDirectory, String... exemptFiles) throws IOException {
		Set<String> exemptFilesSet = new HashSet<>(Arrays.asList(exemptFiles));
		try (ZipInputStream zis = new ZipInputStream(new BufferedInputStream(new FileInputStream(zipDirectory.toString())))) {
			ZipEntry entry = null;
			while ((entry = zis.getNextEntry()) != null) {
				extractEntryContents(zis, entry, outputDirectory.toString(), exemptFilesSet);
			}
			zis.closeEntry();
		}
	}

	public static void zipFile(String fileToZip, String destinationFilename) throws IOException {
		try (ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(destinationFilename))) {
			File file = new File(fileToZip);
			zipFile(file, file.getName(), zipOut);
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

	public static void extractFiles(String zipDirectory, String outputDirectory, String... exemptFiles) throws IOException {
		Set<String> exemptFilesSet = new HashSet<>(Arrays.asList(exemptFiles));
		try (ZipInputStream zis = new ZipInputStream(new BufferedInputStream(new FileInputStream(zipDirectory)))) {
			ZipEntry entry = null;
			while ((entry = zis.getNextEntry()) != null) {
				extractEntryContents(zis, entry, outputDirectory, exemptFilesSet);
			}
			zis.closeEntry();
		}
	}

	private static void extractEntryContents(ZipInputStream zis, ZipEntry entry, String outputDirectory, Set<String> exemptFiles) throws IOException {
		Path path = Paths.get(outputDirectory, entry.getName());
		if (!exemptFiles.contains(path.toString())) {
			Files.createDirectories(path.getParent());
			try (BufferedOutputStream bos = new BufferedOutputStream(Files.newOutputStream(path))) {
				copyBytes(zis, bos);
			}
		}
	}

	private static void copyBytes(InputStream inputStream, OutputStream outputStream) throws IOException {
		byte[] buffer = new byte[1024];
		int bytesRead;
		while ((bytesRead = inputStream.read(buffer)) != -1) {
			outputStream.write(buffer, 0, bytesRead);
		}
	}

}