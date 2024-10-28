package org.bluedb.disk.file;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;

import org.bluedb.api.exceptions.BlueDbException;

public class BlueDataInputStream implements BlueInputStream {
	
	private String description;
	private DataInputStream dataInputStream;
	private long totalBytesInStream = -1;
	
	public BlueDataInputStream(InputStream inputStream) {
		this.description = "input stream " + inputStream;
		this.dataInputStream = new DataInputStream(inputStream);
	}

	public BlueDataInputStream(Path path) throws BlueDbException {
		this(path.toFile());
	}
	
	public BlueDataInputStream(File file) throws BlueDbException {
		try {
			this.description = "file " + file;
			this.dataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
			this.totalBytesInStream = file.length();
		} catch (Throwable t) {
			throw new BlueDbException("Failed to create data input stream for " + description, t);
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
			return dataInputStream.read();
		} catch (Throwable t) {
			throw new BlueDbException("Failed to read next byte from " + description, t);
		}
	}
	
	@Override
	public int readBytes(byte[] buffer, int dataOffset, int lengthToRead) throws BlueDbException {
		try {
			return dataInputStream.read(buffer, dataOffset, lengthToRead);
		} catch (Throwable t) {
			throw new BlueDbException("Failed to read " + lengthToRead + " bytes from " + description, t);
		}
	}
	
	@Override
	public void readFully(byte[] buffer, int dataOffset, int lengthToRead) throws BlueDbException {
		try {
			dataInputStream.readFully(buffer, dataOffset, lengthToRead);
		} catch (Throwable t) {
			throw new BlueDbException("Failed to read " + lengthToRead + " bytes fully from " + description, t);
		}
	}

	@Override
	public void mark(int readLimit) {
		dataInputStream.mark(readLimit);
	}

	@Override
	public void resetToLastMark() throws BlueDbException {
		try {
			dataInputStream.reset();
		} catch (Throwable t) {
			throw new BlueDbException("Failed to reset file reader to previously marked position for " + description, t);
		}
	}

	@Override
	public void close() {
		if(dataInputStream != null) {
			try {
				dataInputStream.close();
			} catch(Throwable t) {
				t.printStackTrace();
			}
		}
	}
	
}
