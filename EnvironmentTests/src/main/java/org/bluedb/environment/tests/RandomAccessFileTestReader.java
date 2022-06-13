package org.bluedb.environment.tests;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class RandomAccessFileTestReader implements OsFileSystemTestWriter {
	private Path testFilePath;
	private RandomAccessFile reader;
	
	private Random r = new Random();

	public void startWriting(Path testFilePath) throws InterruptedException, IOException {
		this.testFilePath = testFilePath;
		int i = 0;
		while(true) {
			sleepForRandomTime();
			createReadWriter();
			System.out.println("Read " + i + " " + readAllIntegers());
			closeReadWriter();
			i++;
		}
	}

	private void createReadWriter() throws FileNotFoundException {
		reader = new RandomAccessFile(testFilePath.toFile(), "r");
	}

	private void closeReadWriter() throws IOException {
		reader.close();
	}

	private List<Integer> readAllIntegers() throws IOException {
		List<Integer> integers = new LinkedList<>();
		reader.seek(0);
		Integer nextInt = readInt();
		while(nextInt != null) {
			integers.add(nextInt);
			nextInt = readInt();
		}
		return integers;
	}
	
	public Integer readInt() throws IOException {
		/*
		 * This is a copy of DataInputStream.readInt except that it returns null if the end of the file was reached
		 * instead of throwing an exception. We noticed that reading through so many files in BlueDB was resulting
		 * in TONS of EOFExceptions being thrown and caught which is a bit heavy. We could return an optional or
		 * something but this is a really low level method that is going to be called a TON so I figured that
		 * it is probably worth just handling a null return rather than creating a new object every time we
		 * call it.
		 */
		int ch1 = reader.read();
		int ch2 = reader.read();
		int ch3 = reader.read();
		int ch4 = reader.read();
		if ((ch1 | ch2 | ch3 | ch4) < 0) {
			return null;
		}
		return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
	}

	private void sleepForRandomTime() throws InterruptedException {
		Thread.sleep(Math.abs(r.nextLong()) % 10);
	}
}
