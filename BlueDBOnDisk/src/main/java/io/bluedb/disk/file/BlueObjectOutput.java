package io.bluedb.disk.file;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.serialization.BlueSerializer;

public class BlueObjectOutput<T> implements Closeable {

	private final Path path;
	private final BlueSerializer serializer;
	private final DataOutputStream dataOutputStream;

	public BlueObjectOutput(BlueWriteLock<Path> writeLock, BlueSerializer serializer) throws BlueDbException {
		Path path = writeLock.getKey();
		this.path = path;
		this.serializer = serializer;
		File file = path.toFile();
		FileManager.ensureDirectoryExists(file);
		dataOutputStream = openDataOutputStream(file);
	}

	// for testing only
	protected BlueObjectOutput(Path path, BlueSerializer serializer, DataOutputStream dataOutputStream) {
		this.path = path;
		this.serializer = serializer;
		this.dataOutputStream = dataOutputStream;
	}

	public void write(T value) throws BlueDbException {
		if (value == null) {
			throw new BlueDbException("cannot write null to " + this.getClass().getSimpleName());
		}
		try {
			byte[] bytes = serializer.serializeObjectToByteArray(value);
			int len = bytes.length;
			dataOutputStream.writeInt(len);
			dataOutputStream.write(bytes);
		} catch (Throwable t) {
			t.printStackTrace();
			throw new BlueDbException("error writing to file " + path, t);
		}
	}

	public void writeAll(BlueObjectInput<T> input) throws BlueDbException {
		while(input.hasNext()) {
			T next = input.next();
			write(next);
		}
	}

	@Override
	public void close() {
		try {
			dataOutputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected static DataOutputStream openDataOutputStream(File file) throws BlueDbException {
		try {
			return new DataOutputStream(new FileOutputStream(file));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new BlueDbException("cannot open write to file " + file.toPath(), e);
		}
	}
}