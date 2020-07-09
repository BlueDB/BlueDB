package org.bluedb.disk.file;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.lock.BlueReadLock;
import org.bluedb.disk.lock.LockManager;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.validation.SerializationException;

public class BlueObjectInput<T> implements Closeable, Iterator<T> {

	private final BlueReadLock<Path> readLock;
	private final Path path;
	private final BlueSerializer serializer;
	private final DataInputStream dataInputStream;
			
	private T next = null;
	private byte[] nextBytes = null;
	private byte[] lastBytes = null;

	public BlueObjectInput(BlueReadLock<Path> readLock, BlueSerializer serializer) throws BlueDbException {
		try {
			this.readLock = readLock;
			this.path = readLock.getKey();
			this.serializer = serializer;
		
			if (path.toFile().exists()) {
				dataInputStream = openDataInputStream(path.toFile());
			} else {
				dataInputStream = null;
			}
		} catch(Throwable t) {
			close();
			throw new BlueDbException(t.getMessage(), t);
		}
	}

	protected static <T> BlueObjectInput<T> getTestInput(Path path, BlueSerializer serializer, DataInputStream dataInputStream) {
		return new BlueObjectInput<T>(path, serializer, dataInputStream);
	}

	private BlueObjectInput(Path path, BlueSerializer serializer, DataInputStream dataInputStream) {
		LockManager<Path> lockManager = new LockManager<Path>();
		readLock = lockManager.acquireReadLock(path);
		this.serializer = serializer;
		this.path = null;
		this.dataInputStream = dataInputStream;
	}

	public Path getPath() {
		return path;
	}

	@Override
	public void close() {
		if (dataInputStream != null) {
			try {
				dataInputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		if(readLock != null) {
			readLock.close();
		}
	}

	@Override
	public boolean hasNext() {
		return peek() != null;
	}

	@Override
	public T next() {
		if (next == null) {
			next = nextValidObjectFromFile();
		}
		T response = next;
		lastBytes = nextBytes;
		next = null;
		nextBytes = null;
		return response;
	}

	public byte[] nextWithoutDeserializing() {
		if (next == null) {
			nextBytes = nextBytesFromFile();
		}  // otherwise you've already peeked ahead
		lastBytes = nextBytes;
		byte[] response = nextBytes;
		next = null;
		nextBytes = null;
		return response;
	}

	public byte[] getLastBytes() {
		return lastBytes;
	}

	public T peek() {
		if (next == null) {
			next = nextValidObjectFromFile();
		}
		return next;
	}

	private T nextValidObjectFromFile() {
		while(true) {
			try {
				return nextFromFile();
			} catch(SerializationException t) {
				t.printStackTrace(); // Object was corrupted. Print stack trace but try loading the next one
			}
		}
	}

	private T nextFromFile() throws SerializationException {
		nextBytes = nextBytesFromFile();
		if (nextBytes == null) {
			return null;
		}
		Object object = serializer.deserializeObjectFromByteArray(nextBytes);
		@SuppressWarnings("unchecked")
		T t = (T) object;
		return t;
	}

	protected byte[] nextBytesFromFile() {
		if (dataInputStream == null) {
			return null;
		}
		try {
			Integer objectLength = readInt();
			if(objectLength == null) {
				return null; //This means that we've reached the end of the file
			}
			if(objectLength <= 0) {
				System.out.println("BlueDB Error: We just read in an object size of " + objectLength + " which doesn't make sense. We will skip this file since it must be corrupt: " + path);
				return null;
			}
			byte[] nextBytes = new byte[objectLength];
			dataInputStream.readFully(nextBytes, 0, objectLength);
			return nextBytes;
		} catch (EOFException e) {
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/*
	 * This is a copy of DataInputStream.readInt except that it returns null if the end of the file was reached
	 * instead of throwing an exception. We noticed that reading through so many files in BlueDB was resulting
	 * in TONS of EOFExceptions being thrown and caught which is a bit heavy. We could return an optional or
	 * something but this is a really low level method that is going to be called a TON so I figured that
	 * it is probably worth just handling a null return rather than creating a new object every time we
	 * call it.
	 */
	private Integer readInt() throws IOException {
        int ch1 = dataInputStream.read();
        int ch2 = dataInputStream.read();
        int ch3 = dataInputStream.read();
        int ch4 = dataInputStream.read();
        if ((ch1 | ch2 | ch3 | ch4) < 0)
            return null;
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

	protected static DataInputStream openDataInputStream(File file) throws IOException {
		return new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
	}
}