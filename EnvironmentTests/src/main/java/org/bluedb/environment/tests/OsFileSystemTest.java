package org.bluedb.environment.tests;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/*
 * Windows 10 Findings:
 * Random Access readers and writers can be open at the same time. Since the writer process takes longer, the reader will
 * pass it up. When this happens, you'll see two of the same number back to back, since the writer is currently working
 * on updating one of them. So the reader gets all correct objects except for one. I'm worried with complex objects that
 * the reader could get a corrupt object if the object is partially written to. I bet that if we re-read in the bytes and
 * tried again then it would fix the issue though...
 */

public class OsFileSystemTest {
	public static void main(String[] args) throws InterruptedException, IOException {
		Path testFilePath = createTestFilePath();
		if("startWriter".equals(args[0])) {
			System.out.println("Starting Writer for test file: " + testFilePath);
			RandomAccessFileTestWriter writer = new RandomAccessFileTestWriter();
			writer.startWriting(testFilePath);
		} else if("startReader".equals(args[0])) {
			System.out.println("Starting Reader for test file: " + testFilePath);
			RandomAccessFileTestReader writer = new RandomAccessFileTestReader();
			writer.startWriting(testFilePath);
		}
	}

	private static Path createTestFilePath() {
		Path testFile = Paths.get("OsFileSystemTest.bin");
		return testFile;
	}
}
