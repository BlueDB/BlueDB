package io.bluedb.disk.segment;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.disk.file.FileManager;
import io.bluedb.disk.serialization.BlueSerializer;

public class Segment <T extends Serializable> {

	private final FileManager fileManager;
	private final Path segmentPath;

	public Segment(Path segmentPath, FileManager fileManager) {
		this.segmentPath = segmentPath;
		this.fileManager = fileManager;
	}

	@Override
	public String toString() {
		return "<Segment for path " + segmentPath.toString() + ">";
	}

	public boolean contains(BlueKey key) throws BlueDbException {
		File file = getFileFor(key);
		if (!file.exists()) {
			return false;
		}
		Map<BlueKey, T> fileContents = fetchAsMap(file);
		return fileContents.containsKey(key);
	}

	public void put(BlueKey key, T value) throws BlueDbException {
		File file = getFileFor(key);
		Map<BlueKey, T> fileContents = fetchAsMap(file);
		fileContents.put(key, value);
		persist(file, fileContents);
	}

	public void delete(BlueKey key) throws BlueDbException {
		File file = getFileFor(key);
		if (file.exists()) {
			Map<BlueKey, T> fileContents = fetchAsMap(file);
			fileContents.remove(key);
			persist(file, fileContents);
		}
	}

	public T get(BlueKey key) throws BlueDbException {
		File file = getFileFor(key);
		Map<BlueKey, T> fileContents = fetchAsMap(file);
		return fileContents.get(key);
	}

	public List<T> getAll() throws BlueDbException {
		File[] filesInFolder = segmentPath.toFile().listFiles();
		List<T> results = new ArrayList<>();
		for (File file: filesInFolder) {
			Map<BlueKey, T> fileContents = fetchAsMap(file);
			results.addAll(fileContents.values());
		}
		return results;
	}

    public List<BlueEntity<T>> getRange(long minTime, long maxTime) throws BlueDbException {
		File[] filesInFolder = segmentPath.toFile().listFiles();
		List<BlueEntity<T>> results = new ArrayList<>();
		for (File file: filesInFolder) {
			List<BlueEntity<T>> fileContents = fetch(file);
			for (BlueEntity<T> entity: fileContents) {
				BlueKey key = entity.getKey();
				if (inTimeRange(minTime, maxTime, key)) {
					results.add(entity);
				}
			}
		}
		return results;
	}

	protected File getFileFor(BlueKey key) {
		return getPathFor(key).toFile();
	}

	protected Path getPath() {
		return segmentPath;
	}

	private Path getPathFor(BlueKey key) {
		String fileName = String.valueOf(key.getGroupingNumber());
		return Paths.get(segmentPath.toString(), fileName);
	}

	private TreeMap<BlueKey, T> fetchAsMap(File file) throws BlueDbException {
		List<BlueEntity<T>> entities = fetch(file);
		return asMap(entities);
	}

	// TODO handle locking?
	@SuppressWarnings("unchecked")
	private List<BlueEntity<T>> fetch(File file) throws BlueDbException {
		if (!file.exists())
			return new ArrayList<BlueEntity<T>>();
		List<BlueEntity<T>> fileContents =  (ArrayList<BlueEntity<T>>) fileManager.loadObject(file.toPath());
		if (fileContents == null)
			return new ArrayList<BlueEntity<T>>();
		return fileContents;
	}

	// TODO handle locking?
	private void persist(File file, Map<BlueKey, T> data) throws BlueDbException {
		if (data.isEmpty()) {
			file.delete();
		} else {
			ArrayList<BlueEntity<T>> entites = asEntityArrayList(data);
			fileManager.saveObject(file.toPath(), entites);
		}
	}

	private TreeMap<BlueKey, T> asMap(List<BlueEntity<T>> list) {
		TreeMap<BlueKey, T> map = new TreeMap<>();
		for (BlueEntity<T> entity: list) {
			map.put(entity.getKey(), entity.getObject());
		}
		return map;
	}

	private ArrayList<BlueEntity<T>> asEntityArrayList(Map<BlueKey, T> data) {
		ArrayList<BlueEntity<T>> list = new ArrayList<>();
		for (Entry<BlueKey, T> entry: data.entrySet()) {
			BlueEntity<T> entity = new BlueEntity<T>(entry.getKey(), entry.getValue());
			list.add(entity);
		}
		return list;
	}

	private static boolean inTimeRange(long minTime, long maxTime, BlueKey key) {
		if (key instanceof TimeFrameKey) {
			TimeFrameKey timeFrameKey = (TimeFrameKey) key;
			return timeFrameKey.getEndTime() >= minTime && timeFrameKey.getStartTime() <= maxTime;
		} else {
			return key.getGroupingNumber() >= minTime && key.getGroupingNumber() <= maxTime;
		}
	}

	@Override
	public int hashCode() {
		return 31 + ((segmentPath == null) ? 0 : segmentPath.hashCode());
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Segment)) {
			return false;
		}
		Segment<?> other = (Segment<?>) obj;
		if (segmentPath == null) {
			return other.segmentPath == null;
		} else {
			return segmentPath.equals(other.segmentPath);
		}
	}
}
