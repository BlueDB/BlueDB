package io.bluedb.disk;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.serialization.BlueSerializer;

public class FileManager {
	private final BlueSerializer serializer;

	public FileManager(BlueSerializer serializer) {
		this.serializer = serializer;
	}

	public List<File> listFiles(Path path, String suffix) {
		File folder = path.toFile();
		if (!folder.exists()) {
			return new ArrayList<>();
		}
		File[] filesInFolder = folder.listFiles();
		List<File> results = new ArrayList<>();
		for (File file: filesInFolder) {
			if(file.getName().endsWith(suffix)) {
				results.add(file);
			}
		}
		return results;
	}

	public Object loadObject(Path path) throws BlueDbException {
		byte[] fileData = load(path);
		return serializer.deserializeObjectFromByteArray(fileData);
	}

	public byte[] load(Path path) throws BlueDbException {
		File file = path.toFile();
		if (!file.exists())
			return null;
		try {
			return Files.readAllBytes(path);
		} catch (IOException e) {
			e.printStackTrace();
			// TODO delete the file ?
			throw new BlueDbException("error writing to disk (" + path +")", e);
		}
	}

	public void save(Path path, Object o) throws BlueDbException {
		File file = path.toFile();
		file.getParentFile().mkdirs();
		byte[] bytes = serializer.serializeObjectToByteArray(o);
		try (FileOutputStream fos = new FileOutputStream(file)) {
			fos.write(bytes);
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
			// TODO delete the file
			throw new BlueDbException("error writing to disk (" + path +")", e);
		}
	}
}
