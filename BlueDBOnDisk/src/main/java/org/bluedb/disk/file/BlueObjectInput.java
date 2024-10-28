package org.bluedb.disk.file;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.Iterator;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.encryption.EncryptionServiceWrapper;
import org.bluedb.disk.lock.BlueReadLock;
import org.bluedb.disk.lock.LockManager;
import org.bluedb.disk.metadata.BlueFileMetadata;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.validation.SerializationException;

public class BlueObjectInput<T> implements Closeable, Iterator<T> {

	private final BlueReadLock<Path> readLock;
	private final Path path;
	private final BlueSerializer serializer;
	private final EncryptionServiceWrapper encryptionService;
	private final BlueInputStream blueInputStream;
	private final BlueFileMetadata metadata;

	private T next = null;
	private byte[] nextRawBytes = null;
	private byte[] lastRawBytes = null;
	private byte[] nextUnencryptedBytes = null;
	private byte[] lastUnencryptedBytes = null;
	
	public BlueObjectInput(BlueReadLock<Path> readLock, BlueSerializer serializer, EncryptionServiceWrapper encryptionService) throws BlueDbException {
		this(readLock, serializer, encryptionService, createBlueInputStream(readLock));
	}

	private static BlueInputStream createBlueInputStream(BlueReadLock<Path> readLock) throws BlueDbException {
		if(readLock != null && readLock.getKey() != null && readLock.getKey().toFile().exists()) {
			return new BlueDataInputStream(readLock.getKey().toFile());
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public BlueObjectInput(BlueReadLock<Path> readLock, BlueSerializer serializer, EncryptionServiceWrapper encryptionService, BlueInputStream blueInputStream) throws BlueDbException {
		try {
			this.readLock = readLock;
			this.serializer = serializer;
			this.encryptionService = encryptionService;
			this.blueInputStream = blueInputStream;
			this.path = readLock.getKey();

			setNextBytesFromFile();
			if (nextRawBytes != null) {
				Object firstObject = this.serializer.deserializeObjectFromByteArray(nextUnencryptedBytes);
				if (firstObject instanceof BlueFileMetadata) {
					this.metadata = (BlueFileMetadata) firstObject;
					nextRawBytes = null;
					nextUnencryptedBytes = null;
				} else {
					this.metadata = new BlueFileMetadata();
					next = (T) firstObject;
				}
			} else {
				this.metadata = new BlueFileMetadata();
			}
		} catch (Throwable t) {
			close();
			throw new BlueDbException(t.getMessage(), t);
		}
	}

	protected static <T> BlueObjectInput<T> getTestInput(Path path, BlueSerializer serializer, EncryptionServiceWrapper encryptionService, BlueInputStream blueInputStream, BlueFileMetadata metadata) {
		return new BlueObjectInput<>(path, serializer, encryptionService, blueInputStream, metadata);
	}

	private BlueObjectInput(Path path, BlueSerializer serializer, EncryptionServiceWrapper encryptionService, BlueInputStream blueInputStream, BlueFileMetadata metadata) {
		LockManager<Path> lockManager = new LockManager<>();
		readLock = lockManager.acquireReadLock(path);
		this.serializer = serializer;
		this.encryptionService = encryptionService;
		this.path = null;
		this.blueInputStream = blueInputStream;
		this.metadata = metadata;
	}

	@Override
	public void close() {
		if (blueInputStream != null) {
			blueInputStream.close();
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

	public byte[] nextRawBytesWithoutDeserializing() {
		nextWithoutDeserializing();
		return lastRawBytes;
	}

	public byte[] nextUnencryptedBytesWithoutDeserializing() {
		nextWithoutDeserializing();
		return lastUnencryptedBytes;
	}

	private void nextWithoutDeserializing() {
		if (next == null) {
			setNextBytesFromFile();
		}  // otherwise you've already peeked ahead
		lastRawBytes = nextRawBytes;
		lastUnencryptedBytes = nextUnencryptedBytes;
		next = null;
		nextRawBytes = null;
		nextUnencryptedBytes = null;
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
		Object object = serializer.deserializeObjectFromByteArray(nextUnencryptedBytes);
		@SuppressWarnings("unchecked")
		T t = (T) object;
		return t;
	}

	protected void setNextBytesFromFile() {
		if (blueInputStream == null) {
			nextRawBytes = null;
			nextUnencryptedBytes = null;
			return;
		}
		try {
			blueInputStream.mark(Integer.MAX_VALUE);
			Integer objectLength = blueInputStream.readNextFourBytesAsInt();
			if (isInvalidObjectLength(objectLength)) {
				if(objectLength != null) {
					//Null just means end of file, not error
					System.out.println("BlueDB Error: We just read in an object size of " + objectLength + " which doesn't make sense. We will skip this file since it must be corrupt: " + path);
				}
				nextRawBytes = null;
				nextUnencryptedBytes = null;
				this.blueInputStream.resetToLastMark();
				return;
			}
			blueInputStream.mark(0); // Essentially removes the above mark, we don't want to tell the input stream to store more in it's buffer than is needed.
			byte[] nextBytes = new byte[objectLength];
			blueInputStream.readFully(nextBytes, 0, objectLength);
			nextRawBytes = nextBytes;
			nextUnencryptedBytes = encryptionService.decryptOrReturn(metadata, nextRawBytes);
		} catch (BlueDbException e) {
			e.printStackTrace();
			nextRawBytes = null;
			nextUnencryptedBytes = null;
		}
	}

	private boolean isInvalidObjectLength(Integer objectLength) {
		if(objectLength == null || objectLength <= 0) {
			//Obviously an object can't be 0 or less bytes so we know this file is corrupt
			return true;
		}
		
		if(blueInputStream.getTotalBytesInStream() > 0 && objectLength > blueInputStream.getTotalBytesInStream()) {
            //We know the total length of the input stream and we think the object is larger than that so we know this file is corrupt
            return true;
        }
		
		return false;
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
	
	public static class BlueObjectInputState<T> {
		public T next = null;
		public byte[] nextRawBytes = null;
		public byte[] lastRawBytes = null;
		public byte[] nextUnencryptedBytes = null;
		public byte[] lastUnencryptedBytes = null;
		
		public BlueObjectInputState(T next, byte[] nextRawBytes, byte[] lastRawBytes, byte[] nextUnencryptedBytes, byte[] lastUnencryptedBytes) {
			this.next = next;
			this.nextRawBytes = nextRawBytes;
			this.lastRawBytes = lastRawBytes;
			this.nextUnencryptedBytes = nextUnencryptedBytes;
			this.lastUnencryptedBytes = lastUnencryptedBytes;
		}
	}
	
	public BlueObjectInputState<T> getState() {
		return new BlueObjectInputState<>(next, nextRawBytes, lastRawBytes, nextUnencryptedBytes, lastUnencryptedBytes);
	}
	
	public void setState(BlueObjectInputState<T> state) {
		this.next = state.next;
		this.nextRawBytes = state.nextRawBytes;
		this.lastRawBytes = state.lastRawBytes;
		this.nextUnencryptedBytes = state.nextUnencryptedBytes;
		this.lastUnencryptedBytes = state.lastUnencryptedBytes;
	}

}