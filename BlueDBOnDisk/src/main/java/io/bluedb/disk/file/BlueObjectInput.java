package io.bluedb.disk.file;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.serialization.BlueSerializer;

public class BlueObjectInput<T> implements Closeable, Iterator<T> {

	private final Path path;
	private final BlueSerializer serializer;
	private final DataInputStream dataInputStream;
			
	private T next = null;

	public BlueObjectInput(BlueReadLock<Path> readLock, BlueSerializer serializer) throws BlueDbException {
		this.path = readLock.getKey();
		this.serializer = serializer;
		dataInputStream = openDataInputStream(path.toFile());
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
	}

	@Override
	public boolean hasNext() {
		if (next == null) {
			next = nextFromFile();
		}
		return next != null;
	}

	@Override
	public T next() {
		if (next == null) {
			next = nextFromFile();
		}
		T response = next;
		next = null;
		return response;
	}

	protected T nextFromFile() {
		int objectLength;
		try {
			objectLength = dataInputStream.readInt();
			byte[] buffer = new byte[objectLength];
			dataInputStream.readFully(buffer,0, objectLength);
			Object object = serializer.deserializeObjectFromByteArray(buffer);
			@SuppressWarnings("unchecked")
			T t = (T) object;
			return t;
		} catch (EOFException e) {
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	protected static DataInputStream openDataInputStream(File file) throws BlueDbException {
		try {
			return new DataInputStream(new FileInputStream(file));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new BlueDbException("cannot open input stream on file " + file.toPath(), e);
		}
	}
}