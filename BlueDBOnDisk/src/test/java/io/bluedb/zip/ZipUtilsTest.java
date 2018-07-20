package io.bluedb.zip;

import static org.junit.Assert.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Test;

public class ZipUtilsTest {

	@Test
	public void test_constructor() {
		new ZipUtils(); // this doesn't really test anything, it just makes code coverage 100%
	}

	@Test
	public void test() {
		try {
			Path tempFolder = Files.createTempDirectory(this.getClass().getSimpleName());
			tempFolder.toFile().deleteOnExit();
			
			Path folderToCopy = Paths.get(tempFolder.toString(), "folder");
			Path fileInFolderToCopy = Paths.get(folderToCopy.toString(), "file");
			Path subfolderToCopy = Paths.get(folderToCopy.toString(), "subfolder");
			Path fileInSubfolderToCopy = Paths.get(subfolderToCopy.toString(), "subfolder_file");
			
			folderToCopy.toFile().mkdirs();
			fileInFolderToCopy.toFile().createNewFile();
			subfolderToCopy.toFile().mkdirs();
			fileInSubfolderToCopy.toFile().createNewFile();
			
			Path zippedPath = Paths.get(tempFolder.toString(), "test.zip");
			ZipUtils.zipFile(folderToCopy, zippedPath);
	
			Path unzippedFolder = Paths.get(tempFolder.toString(), "unzipped");
			ZipUtils.extractFiles(zippedPath, unzippedFolder);
			
			Path unzippedFolderToCopy = translatePath(folderToCopy, tempFolder, unzippedFolder);
			Path unzippedFileInFolderToCopy = translatePath(fileInFolderToCopy, tempFolder, unzippedFolder);
			Path unzippedSubfolderToCopy = translatePath(subfolderToCopy, tempFolder, unzippedFolder);
			Path unzippedFileInSubfolderToCopy = translatePath(fileInSubfolderToCopy, tempFolder, unzippedFolder);
			
			assertTrue(unzippedFolderToCopy.toFile().exists());
			assertTrue(unzippedFileInFolderToCopy.toFile().exists());
			assertTrue(unzippedSubfolderToCopy.toFile().exists());
			assertTrue(unzippedFileInSubfolderToCopy.toFile().exists());
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
	}

	public Path translatePath(Path targetPath, Path from, Path to) {
		Path relativePath = from.relativize(targetPath);
		return Paths.get(to.toString(), relativePath.toString());
	}
}
