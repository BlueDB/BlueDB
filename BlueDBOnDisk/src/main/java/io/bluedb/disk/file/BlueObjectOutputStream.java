package io.bluedb.disk.file;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.serialization.BlueSerializer;

public class BlueObjectOutputStream<T> implements Closeable {

	private final Path tempFilePath;
	private final Path targetFilePath;
	private final BlueSerializer serializer;
	private final FileManager fileManager;
	private final DataOutputStream dataOutputStream;

	public BlueObjectOutputStream(Path path, BlueSerializer serializer, FileManager fileManager) throws BlueDbException {
		this.tempFilePath = FileManager.createTempFilePath(path);
		this.targetFilePath = path;
		this.serializer = serializer;
		this.fileManager = fileManager;
		File tempFile = tempFilePath.toFile();
		FileManager.ensureDirectoryExists(tempFile);
		dataOutputStream = openDataOutputStream(tempFile);
	}

	public void write(T value) throws BlueDbException {
		// TODO early return or exception on nulls ?
		try {
			byte[] bytes = serializer.serializeObjectToByteArray(value);
			int len = bytes.length;
			dataOutputStream.writeInt(len);
			dataOutputStream.write(bytes);
		} catch (Throwable t) {
			t.printStackTrace();
			throw new BlueDbException("error writing to file " + targetFilePath, t);
		}
	}

	@Override
	public void close() throws IOException {
		dataOutputStream.close();
	}

	public void commit() throws BlueDbException, IOException {
		close();
		fileManager.lockMoveFileUnlock(tempFilePath, targetFilePath);
	}

	private DataOutputStream openDataOutputStream(File file) throws BlueDbException {
		try {
			return new DataOutputStream(new FileOutputStream(file));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new BlueDbException("cannot open write to file " + tempFilePath, e);
		}
	}
}
