package io.bluedb.disk.file;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.serialization.BlueSerializer;

public class BlueObjectInputStream<T> implements Closeable {

	private final Path path;
	private final BlueSerializer serializer;
	private final DataInputStream dataInputStream;
			
	public BlueObjectInputStream(BlueReadLock<Path> readLock, BlueSerializer serializer) throws BlueDbException {
		this.path = readLock.getKey();
		this.serializer = serializer;
		dataInputStream = openDataInputStream(path.toFile());
	}

	public T next() throws EOFException, BlueDbException {
		int objectLength;
		try {
			objectLength = dataInputStream.readInt();
			byte[] buffer = new byte[objectLength];
			int bytesRead = dataInputStream.read(buffer,0, objectLength);
			if (bytesRead != objectLength) {
				// TODO throw an exception ?
			}
			Object object = serializer.deserializeObjectFromByteArray(buffer);
			@SuppressWarnings("unchecked")
			T t = (T) object;
			return t;
		} catch (EOFException e) {
			throw e;
			// TODO or return null?
		} catch (IOException e) {
			e.printStackTrace();
			throw new BlueDbException("error reading next from file " + path, e);
		}
	}

	@Override
	public void close() throws IOException {
		if (dataInputStream != null) {
			dataInputStream.close();
		}
	}

	private DataInputStream openDataInputStream(File file) throws BlueDbException{
		try {
			return new DataInputStream(new FileInputStream(file));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new BlueDbException("cannot open input stream on file " + path, e);
		}
	}
}