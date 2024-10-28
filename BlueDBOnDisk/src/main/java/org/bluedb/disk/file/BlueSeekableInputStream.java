package org.bluedb.disk.file;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Path;

import org.bluedb.api.exceptions.BlueDbException;

public class BlueSeekableInputStream implements BlueInputStream {
	private String description;
	private RandomAccessFile randomAccessFile;
	private long totalBytesInStream = -1;
	
	private long markedPosition = 0;
	
	public BlueSeekableInputStream(RandomAccessFile randomAccessFile) throws BlueDbException {
		try {
			this.description = "file " + randomAccessFile;
			this.randomAccessFile = randomAccessFile;
			this.totalBytesInStream = randomAccessFile.length();
		} catch (Throwable t) {
			throw new BlueDbException("Failed to create BlueSeekableInputStream for file " + randomAccessFile, t);
		}
	}
	
	public BlueSeekableInputStream(Path path) throws BlueDbException {
		this(path.toFile());
	}
	
	public BlueSeekableInputStream(File file) throws BlueDbException {
		try {
			this.description = "file " + file;
			this.randomAccessFile = new RandomAccessFile(file, "r");
			this.totalBytesInStream = file.length();
		} catch (Throwable t) {
			throw new BlueDbException("Failed to create BlueSeekableInputStream for file " + file, t);
		}
	}

	@Override
	public String getDescription() {
		return description;
	}

	@Override
	public long getTotalBytesInStream() {
		return totalBytesInStream;
	}

	@Override
	public int readNextByteAsInt() throws BlueDbException {
		try {
			return randomAccessFile.read();
		} catch (Throwable t) {
			throw new BlueDbException("Failed to read next byte from " + getDescription(), t);
		}
	}
	
	@Override
	public int readBytes(byte[] buffer, int dataOffset, int lengthToRead) throws BlueDbException {
		try {
			return randomAccessFile.read(buffer, dataOffset, lengthToRead);
		} catch (Throwable t) {
			throw new BlueDbException("Failed to read " + lengthToRead + " bytes from " + getDescription(), t);
		}
	}
	
	@Override
	public void readFully(byte[] buffer, int dataOffset, int lengthToRead) throws BlueDbException {
		try {
			randomAccessFile.readFully(buffer, dataOffset, lengthToRead);
		} catch (Throwable t) {
			throw new BlueDbException("Failed to read " + lengthToRead + " bytes fully from " + getDescription(), t);
		}
	}

	@Override
	public void mark(int readLimit) throws BlueDbException {
		try {
			markedPosition = randomAccessFile.getFilePointer();
		} catch (Throwable t) {
			throw new BlueDbException("Failed to mark the cursor position in " + getDescription(), t);
		}
	}

	@Override
	public void resetToLastMark() throws BlueDbException {
		try {
			randomAccessFile.seek(markedPosition);
		} catch (Throwable t) {
			throw new BlueDbException("Failed to set the cursor position to the last marked position " + getDescription(), t);
		}
	}

	@Override
	public void close() {
		if(randomAccessFile != null) {
			try {
				randomAccessFile.close();
			} catch(Throwable t) {
				t.printStackTrace();
			}
		}
	}
	
	public long getCursorPosition() throws BlueDbException {
		try {
			return randomAccessFile.getFilePointer();
		} catch (Throwable t) {
			throw new BlueDbException("Failed to get the cursor position for the " + getDescription(), t);
		}
	}
	
	public void setCursorPosition(long cursorPosition) throws BlueDbException {
		try {
			randomAccessFile.seek(cursorPosition);
		} catch (Throwable t) {
			throw new BlueDbException("Failed to set the cursor position for the " + getDescription(), t);
		}
	}
}
