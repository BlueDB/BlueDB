package org.bluedb.disk.file;

import java.io.ByteArrayOutputStream;

import org.bluedb.api.exceptions.BlueDbException;

public interface BlueInputStream extends AutoCloseable {
	
	public String getDescription();
	
	/**
	 * Reads the next four bytes as an integer. Returns null instead of throwing an exception if it 
	 * reaches the end of the file.
	 * @throws BlueDbException
	 */
	public default Integer readNextFourBytesAsInt() throws BlueDbException {
		/*
		 * This is a copy of DataInputStream.readInt except that it returns null if the end of the file was reached
		 * instead of throwing an exception. We noticed that reading through so many files in BlueDB was resulting
		 * in TONS of EOFExceptions being thrown and caught which is a bit heavy. We could return an optional or
		 * something but this is a really low level method that is going to be called a TON so I figured that
		 * it is probably worth just handling a null return rather than creating a new object every time we
		 * call it.
		 */
		int ch1 = readNextByteAsInt();
		int ch2 = readNextByteAsInt();
		int ch3 = readNextByteAsInt();
		int ch4 = readNextByteAsInt();
		if ((ch1 | ch2 | ch3 | ch4) < 0) {
			return null;
		}
		return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
	}

	public int readNextByteAsInt() throws BlueDbException;
	
	public default byte[] readAllRemainingBytes() throws BlueDbException {
		try {
			ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			int numBytesRead;
			byte[] nextBytes = new byte[1024];
			while ((numBytesRead = readBytes(nextBytes, 0, nextBytes.length)) != -1) {
				buffer.write(nextBytes, 0, numBytesRead);
			}
	
			buffer.flush();
			return buffer.toByteArray();
		} catch (Throwable t) {
			throw new BlueDbException("Failed to read all remaining bytes from " + getDescription(), t);
		}
	}

	public int readBytes(byte[] buffer, int dataOffset, int lengthToRead) throws BlueDbException;

	public void readFully(byte[] buffer, int dataOffset, int lengthToRead) throws BlueDbException;
	
	/**
	 * Marks the current position in this input stream. A subsequent call to the reset method repositions this 
	 * stream at the last marked position so that subsequent reads re-read the same bytes. The read limit argument 
	 * tells this input stream to allow that many bytes to be read before the mark position gets invalidated.
	 * @param readLimit the maximum limit of bytes that can be read beforethe mark position becomes invalid.
	 */
	public void mark(int readLimit) throws BlueDbException;
	
	/**
	 * Repositions this stream to the position at the time the mark method was last called on this input stream. 
	 * This method simply performs in.reset(). Stream marks are intended to be used in situations where you need 
	 * to read ahead a little to see what's in the stream. Often this is most easily done by invoking some general 
	 * parser. If the stream is of the type handled by the parse, it just chugs along happily. If the stream is 
	 * not of that type, the parser should toss an exception when it fails.If this happens within readlimit bytes, 
	 * it allows the outercode to reset the stream and try another parser.
	 */
	public void resetToLastMark() throws BlueDbException;
	
	@Override
	public void close(); //Don't want close to throw an IOException

}
