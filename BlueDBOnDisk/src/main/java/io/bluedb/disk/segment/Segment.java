package io.bluedb.disk.segment;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;
import org.nustaq.serialization.FSTConfiguration;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.Blutils;

public class Segment <T extends Serializable> {
	private static String SUFFIX = ".segment";

	final String pathString;
	final Path path;
	private static final FSTConfiguration serializer = FSTConfiguration.createDefaultConfiguration();

	public Segment(Path collectionPath, String segmentId) {
		this.path = Paths.get(collectionPath.toString(), segmentId + SUFFIX);
		this.pathString = this.path.toString();
	}

	public Segment(Path collectionPath, long segmentId) {
		this.path = Paths.get(collectionPath.toString(), segmentId + SUFFIX);
		this.pathString = this.path.toString();
	}

	public void put(BlueKey key, T value) throws BlueDbException {
		TreeMap<BlueKey, BlueEntity<T>> data = load();
		BlueEntity<T> entity = new BlueEntity<T>(key, value);
		data.put(key, entity);
		Blutils.save(pathString, data);
	}

	public void delete(BlueKey key) throws BlueDbException {
		TreeMap<BlueKey, BlueEntity<T>> data = load();
		data.remove(key);
		Blutils.save(pathString, data);
	}

	public BlueEntity<T> read(BlueKey key) throws BlueDbException {
		Collection<BlueEntity<T>> values = load().values();
		for (BlueEntity<T> entity: values) {
			if (entity.getKey().equals(key))
				return entity;
		}
		return null;
	}

	public List<BlueEntity<T>> read() throws BlueDbException {
		List<BlueEntity<T>> results = new ArrayList<>(load().values());
		return results;
	}

	public List<BlueEntity<T>> read(long minTime, long maxTime) throws BlueDbException {
		List<BlueEntity<T>> results = new ArrayList<>();
		for (BlueEntity<T>entity: load().values()) {
			if (Blutils.meetsTimeConstraint(entity.getKey(), minTime, maxTime))
				results.add(entity);
		}
		return results;
	}

	@SuppressWarnings("unchecked")
	private TreeMap<BlueKey, BlueEntity<T>> load() throws BlueDbException {
		try {
			File file = new File(pathString);
			if (!file.exists()) {
				return new TreeMap<>();
			} else {
				byte[] bytes = Files.readAllBytes(Paths.get(pathString));
				return (TreeMap<BlueKey, BlueEntity<T>>) serializer.asObject(bytes);
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new BlueDbException("error loading segment from disk", e);
		}
	}

	@Override
	public String toString() {
		return "<Segment for path " + pathString + ">";
	}
}
