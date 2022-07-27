package org.bluedb.environment.tests;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Random;

public class RandomAccessFileTestWriter implements OsFileSystemTestWriter {
	private static final int MIN_COUNT = 1200;
	private static final int MAX_COUNT = 20;
	
	private Path testFilePath;
	private RandomAccessFile readWriter;
	
	private Random r = new Random();
	private boolean growing = true;

	public void startWriting(Path testFilePath) throws InterruptedException, IOException {
		this.testFilePath = testFilePath;
		createReadWriter();
		writeInitialIntegers();
		closeReadWriter();
		
		while(true) {
			sleepForRandomTime();
			createReadWriter();
			incrementEachInteger();
			closeReadWriter();
		}
	}

	private void createReadWriter() throws FileNotFoundException {
		readWriter = new RandomAccessFile(testFilePath.toFile(), "rwd");
	}

	private void closeReadWriter() throws IOException {
		readWriter.close();
	}

	private void writeInitialIntegers() throws IOException {
		for(int i = 0; i < MIN_COUNT; i++) {
			writeInt(i);
		}
	}

	private void incrementEachInteger() throws IOException {
		readWriter.seek(0);
		Integer nextInt = readInt();
		while(nextInt != null) {
			readWriter.seek(readWriter.getFilePointer() - 4);
			readWriter.writeInt(nextInt+1);
			nextInt = readInt();
		}
	}
	
	private void writeInt(int v) throws IOException {
		readWriter.write((v >>> 24) & 0xFF);
		readWriter.write((v >>> 16) & 0xFF);
		readWriter.write((v >>>  8) & 0xFF);
		readWriter.write((v >>>  0) & 0xFF);
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
		int ch1 = readWriter.read();
		int ch2 = readWriter.read();
		int ch3 = readWriter.read();
		int ch4 = readWriter.read();
		if ((ch1 | ch2 | ch3 | ch4) < 0) {
			return null;
		}
		return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
	}

	private void sleepForRandomTime() throws InterruptedException {
		Thread.sleep(Math.abs(r.nextLong()) % 10);
	}
}
