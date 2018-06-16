package io.bluedb.disk;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.nustaq.serialization.FSTConfiguration;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;

public class Segment {
//	public void put(BlueEntity entity) {
//		// TODO
//	}
//
	
	final static private LockManager lockManager = new LockManager();
	final String path;
	private static final FSTConfiguration serializer = FSTConfiguration.createDefaultConfiguration();

	public  Segment(String path) {
		this.path = path + ".segment"; // TODO
	}

	public void put(BlueKey key, Serializable value) throws BlueDbException {
		lockManager.lock(path);
		TreeMap<BlueKey, BlueEntity> data = load();
		BlueEntity entity = new BlueEntity(key, value);
		data.put(key, entity);
		save(data);
		lockManager.unlock(path);
	}

	public void delete(BlueKey key) throws BlueDbException {
		lockManager.lock(path);
		TreeMap<BlueKey, BlueEntity> data = load();
		data.remove(key);
		save(data);
		lockManager.unlock(path);
	}

	public List<BlueEntity> read() throws BlueDbException {
		lockManager.lock(path);
		List<BlueEntity> results = new ArrayList<>(load().values());
		lockManager.unlock(path);
		return results;
	}

	@SuppressWarnings("unchecked")
	private TreeMap<BlueKey, BlueEntity> load() throws BlueDbException {
		try {
			File file = new File(path);
			if (!file.exists()) {
				return new TreeMap<>();
			} else {
				byte[] bytes = Files.readAllBytes(Paths.get(path));
				return (TreeMap<BlueKey, BlueEntity>) serializer.asObject(bytes);
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new BlueDbException("error loading segment from disk", e);
		}
	}
	
	private void save(Object o) throws BlueDbException {
		try (FileOutputStream fos = new FileOutputStream(path)) {
			byte[] bytes = serializer.asByteArray(o);
		   fos.write(bytes);
		   fos.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new BlueDbException("error loading segment from disk", e);
		}
	}

	@Override
	public String toString() {
		return "<Segment for path " + path + ">";
	}
}
