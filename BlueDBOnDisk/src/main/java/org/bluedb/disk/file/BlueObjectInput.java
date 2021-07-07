package org.bluedb.disk.file;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

import org.bluedb.api.encryption.EncryptionServiceWrapper;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.metadata.BlueFileMetadata;
import org.bluedb.disk.lock.BlueReadLock;
import org.bluedb.disk.lock.LockManager;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.validation.SerializationException;

public class BlueObjectInput<T> implements Closeable, Iterator<T> {

	private final BlueReadLock<Path> readLock;
	private final Path path;
	private final BlueSerializer serializer;
	private final EncryptionServiceWrapper encryptionService;
	private final DataInputStream dataInputStream;
	private final BlueFileMetadata metadata;

	private T next = null;
	private byte[] nextRawBytes = null;
	private byte[] lastRawBytes = null;
	private byte[] nextUnencryptedBytes = null;
	private byte[] lastUnencryptedBytes = null;

	@SuppressWarnings("unchecked")
	public BlueObjectInput(BlueReadLock<Path> readLock, BlueSerializer serializer, EncryptionServiceWrapper encryptionService) throws BlueDbException {
		try {
			this.readLock = readLock;
			this.path = readLock.getKey();
			this.serializer = serializer;
			this.encryptionService = encryptionService;

			if (path.toFile().exists()) {
				dataInputStream = FileUtils.openDataInputStream(path.toFile());
			} else {
				dataInputStream = null;
			}

			setNextBytesFromFile();
			if (nextRawBytes != null) {
				Object firstObject = this.serializer.deserializeObjectFromByteArray(nextUnencryptedBytes);
				if (firstObject != null && firstObject.getClass() == BlueFileMetadata.class) {
					this.metadata = (BlueFileMetadata) firstObject;
				} else {
					this.metadata = null;
					next = (T) firstObject;
				}
			} else {
				this.metadata = null;
			}

		} catch (Throwable t) {
			close();
			throw new BlueDbException(t.getMessage(), t);
		}
	}

	protected static <T> BlueObjectInput<T> getTestInput(Path path, BlueSerializer serializer, EncryptionServiceWrapper encryptionService, DataInputStream dataInputStream, BlueFileMetadata metadata) {
		return new BlueObjectInput<>(path, serializer, encryptionService, dataInputStream, metadata);
	}

	private BlueObjectInput(Path path, BlueSerializer serializer, EncryptionServiceWrapper encryptionService, DataInputStream dataInputStream, BlueFileMetadata metadata) {
		LockManager<Path> lockManager = new LockManager<>();
		readLock = lockManager.acquireReadLock(path);
		this.serializer = serializer;
		this.encryptionService = encryptionService;
		this.path = null;
		this.dataInputStream = dataInputStream;
		this.metadata = metadata;
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

		if (readLock != null) {
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

		lastRawBytes = nextRawBytes;
		lastUnencryptedBytes = nextUnencryptedBytes;
		next = null;
		nextRawBytes = null;
		nextUnencryptedBytes = null;
		return response;
	}

	public byte[] nextWithoutDeserializing() {
		if (next == null) {
			setNextBytesFromFile();
		}  // otherwise you've already peeked ahead
		byte[] response = nextRawBytes;

		lastRawBytes = nextRawBytes;
		lastUnencryptedBytes = nextUnencryptedBytes;
		next = null;
		nextRawBytes = null;
		nextUnencryptedBytes = null;
		return response;
	}

	public T peek() {
		if (next == null) {
			next = nextValidObjectFromFile();
		}
		return next;
	}

	private T nextValidObjectFromFile() {
		while (true) {
			try {
				return nextFromFile();
			} catch (SerializationException t) {
				t.printStackTrace(); // Object was corrupted. Print stack trace but try loading the next one
			}
		}
	}

	private T nextFromFile() throws SerializationException {
		setNextBytesFromFile();
		if (nextRawBytes == null) {
			return null;
		}
		Object object = serializer.deserializeObjectFromByteArray(nextRawBytes);
		@SuppressWarnings("unchecked")
		T t = (T) object;
		return t;
	}

	protected void setNextBytesFromFile() {
		if (dataInputStream == null) {
			nextRawBytes = null;
			nextUnencryptedBytes = null;
			return;
		}
		try {
			Integer objectLength = readInt();
			if (objectLength == null) {
				nextRawBytes = null;
				nextUnencryptedBytes = null;
				return; // This means that we've reached the end of the file
			}
			if (objectLength <= 0) {
				System.out.println("BlueDB Error: We just read in an object size of " + objectLength + " which doesn't make sense. We will skip this file since it must be corrupt: " + path);
				nextRawBytes = null;
				nextUnencryptedBytes = null;
				return;
			}
			byte[] nextBytes = new byte[objectLength];
			dataInputStream.readFully(nextBytes, 0, objectLength);
			nextRawBytes = nextBytes;
			nextUnencryptedBytes = encryptionService.decryptOrReturn(metadata, nextRawBytes);
		} catch (EOFException e) {
			nextRawBytes = null;
			nextUnencryptedBytes = null;
		} catch (IOException e) {
			e.printStackTrace();
			nextRawBytes = null;
			nextUnencryptedBytes = null;
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
		if ((ch1 | ch2 | ch3 | ch4) < 0) {
			return null;
		}
		return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
	}

	public Path getPath() {
		return path;
	}

	public BlueFileMetadata getMetadata() {
		return metadata;
	}

	public byte[] getLastRawBytes() {
		return lastRawBytes;
	}

	public byte[] getLastUnencryptedBytes() {
		return lastUnencryptedBytes;
	}

}