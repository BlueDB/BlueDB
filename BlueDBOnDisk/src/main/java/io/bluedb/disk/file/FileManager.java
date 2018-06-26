package io.bluedb.disk.file;

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
	private final LatchManager<Path> lockManager;

	public FileManager(BlueSerializer serializer) {
		this.serializer = serializer;
		lockManager = new LatchManager<Path>(); 
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
		byte[] fileData = loadBytes(path);
		if (fileData == null || fileData.length == 0) {
			return null;
		}
		return serializer.deserializeObjectFromByteArray(fileData);
	}

	public void saveObject(Path path, Object o) throws BlueDbException {
		File file = path.toFile();
		File parent = file.getParentFile();
		if (parent != null) {
			parent.mkdirs();
		}
		byte[] bytes = serializer.serializeObjectToByteArray(o);
		lockManager.requestLatchFor(path);
		IOException exception = null;
		try {
			try (FileOutputStream fos = new FileOutputStream(file)) {
				fos.write(bytes);
				fos.close();
			} catch (IOException e) {
				exception = e;
				e.printStackTrace();
			}
		} finally {
			lockManager.releaseLatch(path);
		}
		if (exception != null) {
			throw new BlueDbException("error writing to file " + path, exception);
		}
	}

	private byte[] loadBytes(Path path) throws BlueDbException {
		File file = path.toFile();
		byte[] results = null;
		lockManager.requestLatchFor(path);
		IOException exception = null;
		try {
			if (file.exists()) {
				try {
					results = Files.readAllBytes(path);
				} catch (IOException e) {
					exception = e;
					e.printStackTrace();
				}
			}
		} finally {
			lockManager.releaseLatch(path);
		}
		if (exception != null) {
			throw new BlueDbException("error writing to file " + path, exception);
		}
		return results;
	}
}
