package org.bluedb.disk.lock;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Random;

import org.bluedb.disk.file.FileUtils;

public class FileTestUtility {
	public static void main(String[] args) throws IOException {
		String action = args[0];
		switch(action) {
		case "createTestFile": {
			Path testFile = Paths.get(args[1]);
			int testFileSize = Integer.parseInt(args[2]);
			createTestFile(testFile, testFileSize);
			break;
		}
		case "replaceTestFile": {
			Path testFile = Paths.get(args[1]);
			replaceTestFile(testFile);
			break;
		}
		case "deleteTestFile": {
			Path testFile = Paths.get(args[1]);
			deleteTestFile(testFile);
			break;
		}
		case "read": {
			Path testFile = Paths.get(args[1]);
			executeReadAction(testFile);
			break;
		}
		}
		
	}

	private static void createTestFile(Path testFile, int testFileSize) throws IOException {
		System.out.println("Creating test file [size]" + testFileSize + " [path]" + testFile);
		byte[] bytes = new byte[(int)testFileSize]; 
		new Random().nextBytes(bytes);
		Files.write(testFile, bytes);
		boolean success = FileUtils.exists(testFile) && Files.size(testFile) == testFileSize;
		System.out.println("Create Complete [success]" + success);
	}

	private static void replaceTestFile(Path testFile) throws IOException {
		long testFileSize = Files.size(testFile);
		Path tempFile = testFile.getParent().resolve(testFile.getFileName() + ".tmp");
		createTestFile(tempFile, (int)testFileSize);
		byte[] tempBytes = Files.readAllBytes(tempFile);
		
		System.out.println("Temp file created and ready to atomic replace. Press enter when ready");
		System.console().readLine();
		
		System.out.println("Replacing test file " + testFile + " with contents of " + tempFile);
		Files.move(tempFile, testFile, StandardCopyOption.ATOMIC_MOVE);
		boolean success = Arrays.equals(tempBytes, Files.readAllBytes(testFile));
		System.out.println("Replace Complete [success]" + success);
	}

	private static void deleteTestFile(Path testFile) throws IOException {
		System.out.println("Deleting test file " + testFile);
		Files.delete(testFile);
		boolean success = !FileUtils.exists(testFile);
		System.out.println("Delete Complete [success]" + success);
	}

	private static void executeReadAction(Path testFile) throws IOException {
		long testFileSize = Files.size(testFile);
		long bufferSize = (testFileSize / 2) + 1; //We want it to take two reads in order to read the entire file
		byte[] buffer = new byte[(int)bufferSize];
		byte[] results = new byte[(int)testFileSize];
		
		System.out.println("Reading test file " + testFile);
		
		try (FileInputStream in = new FileInputStream(testFile.toFile())) {
			int read1 = in.read(buffer);
			System.arraycopy(buffer, 0, results, 0, read1);
			System.out.println("Read half of the test file, press enter to continue");
			System.console().readLine();
			int read2 = in.read(buffer);
			System.arraycopy(buffer, 0, results, read1, read2);
			System.out.println("Read entire test file, press enter to close input stream.");
			System.console().readLine();
		} catch (Throwable t) {
			t.printStackTrace();
		} finally {
			System.out.println("Read Complete Results (" + results.length + "): " + Arrays.toString(results));
		}
	}
}
